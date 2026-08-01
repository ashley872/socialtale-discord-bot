/**
 * Discord Community Bot — Social Tale
 *
 * Auto-monitors ALL servers the bot is invited to. No per-server config needed.
 * Just invite the bot → it starts tracking immediately.
 *
 * Features:
 * 1. Unanswered message tracking — alerts when community messages go unanswered
 * 2. Proxy posting — team posts as brand via !say command
 * 3. Community metrics — daily snapshots of engagement, retention, growth
 * 4. Daily digest — summary of community health posted to Slack + Discord
 * 5. Auto-onboard — new servers detected on join, Slack notified
 *
 * Run: node bot.cjs
 */

const { Client, GatewayIntentBits, PermissionFlagsBits, EmbedBuilder } = require('discord.js');
const { WebClient } = require('@slack/web-api');
const Anthropic = require('@anthropic-ai/sdk');
const { createClient } = require('@supabase/supabase-js');
const { readFileSync } = require('fs');
const path = require('path');

// On Railway, env vars are set directly. Locally, load from .env.local
require('dotenv').config({ path: path.join(__dirname, '.env.local') });
require('dotenv').config({ path: path.join(__dirname, '../.env.local') });

// ── Config ──────────────────────────────────────────────────────────────────
const CONFIG = JSON.parse(readFileSync(path.join(__dirname, 'discord-servers.json'), 'utf8'));

// Global team identifiers — used across ALL servers
const TEAM_USERNAMES = (CONFIG.teamUsernames || []).map(u => u.toLowerCase());
const TEAM_ROLE_NAME = (CONFIG.teamRoleName || 'Team').toLowerCase();
const RESPONSE_TIME_MINS = CONFIG.responseTimeMinutes || 60;
const ALERT_CHANNEL_NAME = CONFIG.alertChannelName || 'team-alerts';

const supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_KEY);

// Hub Supabase — for reading creator video performance data
const hubSupabase = (process.env.HUB_SUPABASE_URL && process.env.HUB_SUPABASE_KEY)
  ? createClient(process.env.HUB_SUPABASE_URL, process.env.HUB_SUPABASE_KEY)
  : null;

// ── Brand Contexts (AI Auto-Reply) ──────────────────────────────────────────
// Load static JSON as baseline fallback, then poll Hub Supabase every 5 minutes
const STATIC_BRAND_CONTEXTS = JSON.parse(readFileSync(path.join(__dirname, 'brand-contexts.json'), 'utf8'));
const STATIC_DEFAULT = STATIC_BRAND_CONTEXTS._default;

// TikTok Shop knowledge base — loaded into conversational prompts
const TIKTOK_KNOWLEDGE = JSON.parse(readFileSync(path.join(__dirname, 'tiktok-knowledge.json'), 'utf8'));
const TIKTOK_KNOWLEDGE_TEXT = Object.entries(TIKTOK_KNOWLEDGE)
  .map(([category, items]) => `${category.replace(/_/g, ' ').toUpperCase()}:\n${items.map(i => `- ${i}`).join('\n')}`)
  .join('\n\n');

// Mutable cache — updated by DB poll
let defaultBotConfig = { ...STATIC_DEFAULT };
let botConfigCache = new Map(); // discord_server_id → config object
const CONFIG_POLL_MS = 5 * 60 * 1000; // 5 minutes

// Seed cache from static JSON
for (const [k, v] of Object.entries(STATIC_BRAND_CONTEXTS)) {
  if (k === '_default') continue;
  botConfigCache.set(k, v);
}

async function loadBotConfigFromDB() {
  if (!hubSupabase) return;
  try {
    const { data, error } = await hubSupabase
      .from('discord_bot_config')
      .select('*');
    if (error) { console.error('Bot config DB load error:', error.message); return; }
    if (!data || data.length === 0) return; // no rows yet — keep static

    // MERGE DB config on top of static JSON — never wipe static-only fields like `conversational`
    for (const row of data) {
      const dbConfig = {
        brandName:             row.brand_name,
        brandVoice:            row.brand_voice,
        context:               row.context_lines || [],
        faq:                   Object.fromEntries((row.faq || []).map(f => [f.question, f.answer])),
        autoReplyEnabled:      row.auto_reply_enabled,
        autoReplyDelayMinutes: row.auto_reply_delay_mins,
        maxRepliesPerHour:     row.max_replies_per_hour,
        excludeChannels:       row.exclude_channels || [],
        confidenceThreshold:   row.confidence_threshold,
      };
      // Only set conversational if DB explicitly has it; otherwise preserve static JSON value
      if (row.conversational !== undefined && row.conversational !== null) {
        dbConfig.conversational = row.conversational;
      }

      if (row.discord_server_id === '_default') {
        defaultBotConfig = { ...defaultBotConfig, ...dbConfig };
      } else {
        // Merge: static JSON first, then DB overrides on top
        const staticConfig = STATIC_BRAND_CONTEXTS[row.discord_server_id] || {};
        botConfigCache.set(row.discord_server_id, { ...staticConfig, ...dbConfig });
      }
    }
    console.log(`Bot config merged from DB: ${data.length} row(s) applied on top of static JSON`);
  } catch (err) {
    console.error('loadBotConfigFromDB error:', err.message);
  }
}

const anthropic = process.env.ANTHROPIC_API_KEY ? new Anthropic.default({ apiKey: process.env.ANTHROPIC_API_KEY }) : null;

// Track auto-reply rate limiting: serverId → { count, resetTime }
const autoReplyLimits = new Map();
// Track pending auto-replies: messageId → timeoutId (so we can cancel if team replies first)
const pendingAutoReplies = new Map();

// ── Slack ───────────────────────────────────────────────────────────────────
const slack = process.env.SLACK_BOT_TOKEN ? new WebClient(process.env.SLACK_BOT_TOKEN) : null;
const SLACK_CHANNEL = CONFIG.slackChannelId || null;

async function slackAlert(text, blocks) {
  if (!slack || !SLACK_CHANNEL) return;
  try {
    await slack.chat.postMessage({ channel: SLACK_CHANNEL, text, blocks, unfurl_links: false });
  } catch (err) {
    console.error('Slack alert error:', err.message);
  }
}

// ── In-Memory State ─────────────────────────────────────────────────────────
const pendingMessages = new Map();  // serverId → Map<messageId, info>
const dailyActiveUsers = new Map(); // serverId → Set<authorId>
const webhookCache = new Map();     // channelId → Webhook
const channelReplyStreak = new Map(); // channelId → number of consecutive Alice replies

// ── Reactions & Mixing ──────────────────────────────────────────────────────
const REACTION_EMOJIS = {
  intro: ['👋', '🙌', '🤗'],
  achievement: ['🔥', '🎉', '💪', '🙌'],
  question: ['👀', '🤔'],
  positive: ['❤️', '💯', '✨'],
  general: ['👍', '💯', '❤️', '🔥', '✨'],
};

function pickReaction(content) {
  const lower = content.toLowerCase();
  if (/new here|just joined|hey everyone|hi everyone|my name is|i'm new|i am new|intro/.test(lower)) {
    const pool = REACTION_EMOJIS.intro;
    return pool[Math.floor(Math.random() * pool.length)];
  }
  if (/sale|first order|went viral|views|milestone|sold|commission|made \$|earned/.test(lower)) {
    const pool = REACTION_EMOJIS.achievement;
    return pool[Math.floor(Math.random() * pool.length)];
  }
  if (lower.includes('?')) {
    const pool = REACTION_EMOJIS.question;
    return pool[Math.floor(Math.random() * pool.length)];
  }
  if (/love|thank|excited|happy|great|yay|woohoo|can't wait/.test(lower)) {
    const pool = REACTION_EMOJIS.positive;
    return pool[Math.floor(Math.random() * pool.length)];
  }
  const pool = REACTION_EMOJIS.general;
  return pool[Math.floor(Math.random() * pool.length)];
}


// ── Discord Client ──────────────────────────────────────────────────────────
const client = new Client({
  intents: [
    GatewayIntentBits.Guilds,
    GatewayIntentBits.GuildMessages,
    GatewayIntentBits.MessageContent,
    GatewayIntentBits.GuildMembers,
    GatewayIntentBits.GuildMessageReactions,
  ],
});

// ── Helpers ─────────────────────────────────────────────────────────────────
function isTeamMember(member) {
  if (!member) return false;
  // Check by username
  const username = member.user?.username?.toLowerCase();
  if (username && TEAM_USERNAMES.includes(username)) return true;
  // Check by role
  if (member.roles?.cache.some(r => r.name.toLowerCase() === TEAM_ROLE_NAME)) return true;
  return false;
}

async function getOrCreateAlertChannel(guild) {
  let channel = guild.channels.cache.find(c => c.name === ALERT_CHANNEL_NAME && c.type === 0);
  if (!channel) {
    try {
      channel = await guild.channels.create({
        name: ALERT_CHANNEL_NAME,
        type: 0,
        permissionOverwrites: [
          { id: guild.id, deny: [PermissionFlagsBits.ViewChannel] },
          { id: client.user.id, allow: [PermissionFlagsBits.ViewChannel, PermissionFlagsBits.SendMessages] },
        ],
      });
      console.log(`Created #${ALERT_CHANNEL_NAME} in ${guild.name}`);
    } catch (err) {
      console.error(`Could not create #${ALERT_CHANNEL_NAME} in ${guild.name}:`, err.message);
      return null;
    }
  }
  return channel;
}

async function getOrCreateWebhook(channel, brandName) {
  if (webhookCache.has(channel.id)) return webhookCache.get(channel.id);
  try {
    const webhooks = await channel.fetchWebhooks();
    let webhook = webhooks.find(w => w.owner?.id === client.user.id);
    if (!webhook) {
      webhook = await channel.createWebhook({ name: brandName || 'Brand', reason: 'Social Tale proxy posting' });
    }
    webhookCache.set(channel.id, webhook);
    return webhook;
  } catch (err) {
    console.error(`Webhook error in #${channel.name}:`, err.message);
    return null;
  }
}

// ── Message Tracking ────────────────────────────────────────────────────────
async function trackMessage(message, isTeam) {
  const preview = message.content?.substring(0, 100) || '(no text)';
  try {
    await supabase.from('discord_messages').upsert({
      discord_message_id: message.id,
      discord_server_id: message.guild.id,
      discord_channel_id: message.channel.id,
      channel_name: message.channel.name,
      author_id: message.author.id,
      author_name: message.author.displayName || message.author.username,
      is_team_member: isTeam,
      content_preview: preview,
      created_at: message.createdAt.toISOString(),
      is_answered: isTeam,
    }, { onConflict: 'discord_message_id,discord_server_id' });
  } catch (err) {
    console.error('DB track error:', err.message);
  }

  if (!dailyActiveUsers.has(message.guild.id)) dailyActiveUsers.set(message.guild.id, new Set());
  dailyActiveUsers.get(message.guild.id).add(message.author.id);

  if (!isTeam) {
    if (!pendingMessages.has(message.guild.id)) pendingMessages.set(message.guild.id, new Map());
    pendingMessages.get(message.guild.id).set(message.id, {
      channelId: message.channel.id,
      channelName: message.channel.name,
      authorName: message.author.displayName || message.author.username,
      contentPreview: preview,
      timestamp: Date.now(),
      messageUrl: message.url,
    });
  }
}

async function markAsAnswered(message) {
  const serverPending = pendingMessages.get(message.guild.id);
  if (!serverPending) return;

  if (message.reference?.messageId && serverPending.has(message.reference.messageId)) {
    const info = serverPending.get(message.reference.messageId);
    serverPending.delete(message.reference.messageId);
    await supabase.from('discord_messages')
      .update({
        is_answered: true,
        replied_at: new Date().toISOString(),
        replied_by: message.author.displayName || message.author.username,
        reply_time_seconds: Math.floor((Date.now() - info.timestamp) / 1000),
      })
      .eq('discord_message_id', message.reference.messageId)
      .eq('discord_server_id', message.guild.id);
    return;
  }

  for (const [msgId, info] of serverPending.entries()) {
    if (info.channelId === message.channel.id) {
      serverPending.delete(msgId);
      await supabase.from('discord_messages')
        .update({
          is_answered: true,
          replied_at: new Date().toISOString(),
          replied_by: message.author.displayName || message.author.username,
          reply_time_seconds: Math.floor((Date.now() - info.timestamp) / 1000),
        })
        .eq('discord_message_id', msgId)
        .eq('discord_server_id', message.guild.id);
    }
  }
}

// ── Unanswered Check ────────────────────────────────────────────────────────
async function checkUnanswered() {
  const thresholdMs = RESPONSE_TIME_MINS * 60 * 1000;
  const now = Date.now();

  for (const [serverId, serverPending] of pendingMessages.entries()) {
    if (serverPending.size === 0) continue;

    const overdue = [];
    for (const [msgId, info] of serverPending.entries()) {
      if (now - info.timestamp > thresholdMs) overdue.push({ msgId, ...info });
    }
    if (overdue.length === 0) continue;

    const guild = client.guilds.cache.get(serverId);
    if (!guild) continue;

    // Discord alert
    const alertChannel = await getOrCreateAlertChannel(guild);
    if (alertChannel) {
      const embed = new EmbedBuilder()
        .setTitle(`${overdue.length} Unanswered Message${overdue.length > 1 ? 's' : ''}`)
        .setColor(0xFF4444)
        .setTimestamp();

      for (const msg of overdue.slice(0, 10)) {
        const mins = Math.floor((now - msg.timestamp) / 60000);
        embed.addFields({
          name: `#${msg.channelName} — ${msg.authorName} (${mins}m ago)`,
          value: `${msg.contentPreview}\n[Jump to message](${msg.messageUrl})`,
        });
      }
      if (overdue.length > 10) embed.setFooter({ text: `...and ${overdue.length - 10} more` });
      await alertChannel.send({ embeds: [embed] });
    }

    // Slack alert
    const slackBlocks = [
      { type: 'header', text: { type: 'plain_text', text: `${overdue.length} Unanswered in ${guild.name} Discord` } },
      ...overdue.slice(0, 10).map(msg => ({
        type: 'section',
        text: {
          type: 'mrkdwn',
          text: `*#${msg.channelName}* — ${msg.authorName} (${Math.floor((now - msg.timestamp) / 60000)}m ago)\n>${msg.contentPreview}`,
        },
        accessory: { type: 'button', text: { type: 'plain_text', text: 'Open in Discord' }, url: msg.messageUrl },
      })),
    ];
    await slackAlert(`${overdue.length} unanswered message(s) in ${guild.name} Discord`, slackBlocks);

    // Clean up
    for (const msg of overdue) {
      serverPending.delete(msg.msgId);
      await supabase.from('discord_messages')
        .update({ alerted: true })
        .eq('discord_message_id', msg.msgId)
        .eq('discord_server_id', serverId);
    }
    console.log(`Alerted ${overdue.length} unanswered in ${guild.name}`);
  }
}

// ── Proxy Posting ───────────────────────────────────────────────────────────
async function handleProxyCommand(message) {
  if (!message.content.startsWith('!say ')) return false;
  if (!isTeamMember(message.member)) return false;

  const parts = message.content.slice(5).trim();
  let targetChannel = message.channel;
  let text = parts;

  const channelMatch = parts.match(/^<#(\d+)>\s+(.+)$/s);
  if (channelMatch) {
    targetChannel = message.guild.channels.cache.get(channelMatch[1]) || message.channel;
    text = channelMatch[2];
  }

  const webhook = await getOrCreateWebhook(targetChannel, message.guild.name);
  if (!webhook) { await message.reply('Could not create webhook.'); return true; }

  await webhook.send({
    content: text,
    username: message.guild.name,
    avatarURL: message.guild.iconURL({ size: 128 }),
  });

  try { await message.delete(); } catch (_) {}
  console.log(`Proxy posted in #${targetChannel.name} (${message.guild.name})`);
  return true;
}

// ── Status Command ──────────────────────────────────────────────────────────
async function handleStatusCommand(message) {
  if (message.content !== '!community-status') return false;
  if (!isTeamMember(message.member)) return false;

  const serverPending = pendingMessages.get(message.guild.id);
  const pendingCount = serverPending?.size || 0;

  const { data: recent } = await supabase.from('discord_community_metrics')
    .select('*')
    .eq('discord_server_id', message.guild.id)
    .order('snapshot_date', { ascending: false })
    .limit(7);

  const embed = new EmbedBuilder()
    .setTitle(`Community Status — ${message.guild.name}`)
    .setColor(0x5865F2)
    .addFields({ name: 'Pending (unanswered)', value: `${pendingCount} messages`, inline: true })
    .setTimestamp();

  if (recent?.length) {
    const latest = recent[0];
    embed.addFields(
      { name: 'Members', value: `${latest.total_members}`, inline: true },
      { name: 'MAU', value: `${latest.mau}`, inline: true },
      { name: 'Stickiness', value: `${latest.stickiness_ratio}%`, inline: true },
    );
    if (recent.length >= 2) {
      const msgTrend = recent.reduce((a, b) => a + (b.total_messages || 0), 0);
      embed.addFields({ name: '7-Day Messages', value: `${msgTrend}`, inline: true });
    }
  }

  await message.reply({ embeds: [embed] });
  return true;
}

// ── Content Inspiration (/inspo + !inspire + Tue/Thu scheduled) ──────────────
// Top performing creator videos with a full AI breakdown of each: hook, format,
// emotional driver, pain point, difficulty to replicate, and a specific
// adaptation suggestion. Ranked by GMV (views as tiebreak) over the last 7 days.
async function getTopVideosForServer(serverId, limit = 5) {
  if (!hubSupabase) return { videos: [], windowDays: 7 };

  // Look up brand_id from discord_server_id
  const { data: brand } = await hubSupabase
    .from('brands')
    .select('id, name')
    .eq('discord_server_id', serverId)
    .single();

  if (!brand) return { videos: [], windowDays: 7 };

  const fetchWindow = async (days) => {
    const since = new Date(Date.now() - days * 86400000).toISOString().split('T')[0];
    const { data } = await hubSupabase
      .from('creator_videos')
      .select('*')
      .eq('brand_id', brand.id)
      .gte('post_date', since)
      .order('views', { ascending: false })
      .limit(25);
    return (data || []).filter(v => v.video_url && v.views > 0);
  };

  // Last 7 days by default; widen to 30 when the week is thin
  let windowDays = 7;
  let videos = await fetchWindow(7);
  if (videos.length < limit) {
    windowDays = 30;
    videos = await fetchWindow(30);
  }

  // Rank by GMV first (what actually converts), views as tiebreak
  videos.sort((a, b) => (Number(b.gmv) || 0) - (Number(a.gmv) || 0) || (b.views || 0) - (a.views || 0));
  return { videos: videos.slice(0, limit), windowDays };
}

function formatViews(n) {
  if (n >= 1000000) return (n / 1000000).toFixed(1) + 'M';
  if (n >= 1000) return (n / 1000).toFixed(1) + 'K';
  return String(n);
}

function formatMoney(n) {
  const num = Number(n) || 0;
  return '$' + num.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
}

function engagementRate(v) {
  if (!v.views) return null;
  const interactions = (v.likes || 0) + (v.comments || 0) + (v.shares || 0);
  return Math.round((interactions / v.views) * 10000) / 100;
}

// One Claude call analyzes the whole batch — returns array of breakdowns (or null)
async function analyzeTopVideos(videos, brandName) {
  if (!anthropic) return null;

  const summaries = videos.map((v, i) => {
    const lines = [`Video ${i + 1} — @${v.tiktok_account || 'unknown creator'}`];
    // Include whatever descriptive metadata the Euka sync provides
    for (const key of ['video_title', 'title', 'caption', 'description', 'product_name', 'hashtags']) {
      if (v[key]) lines.push(`${key}: ${String(v[key]).substring(0, 300)}`);
    }
    const er = engagementRate(v);
    lines.push(`stats: ${formatViews(v.views)} views, ${formatMoney(v.gmv)} GMV, ${v.orders || 0} orders, ${formatViews(v.likes || 0)} likes, ${v.comments || 0} comments, ${v.shares || 0} shares${er !== null ? `, ${er}% engagement` : ''}`);
    return lines.join('\n');
  }).join('\n\n');

  const prompt = `You are analyzing the top-performing TikTok Shop creator videos for the brand "${brandName}" so other creators in the community can study and adapt what works.

${summaries}

For EACH video, return a JSON object with these keys (each value max 25 words, punchy and specific):
- "whyItWorked": why this video performed, grounded in the stats and metadata given
- "format": the likely video format (prefix with "Likely" if inferring from limited data)
- "hook": the likely opening hook approach
- "emotion": the emotional driver
- "painPoint": the pain point or desire it targets
- "difficulty": exactly one of "easy", "medium", "hard" — how hard to replicate
- "score": integer 1-10, overall replicability-adjusted quality
- "howToAdapt": one specific, actionable suggestion for how a creator should adapt this

Only use the data provided — do not invent details. When inferring, say so ("Likely...").
Respond with ONLY a JSON array of ${videos.length} objects, in the same order as the videos. No other text.`;

  try {
    const response = await anthropic.messages.create({
      model: 'claude-sonnet-4-6',
      max_tokens: 2000,
      messages: [{ role: 'user', content: prompt }],
    });
    const text = response.content[0]?.text || '';
    const start = text.indexOf('[');
    const end = text.lastIndexOf(']');
    if (start === -1 || end === -1) return null;
    const parsed = JSON.parse(text.substring(start, end + 1));
    return Array.isArray(parsed) ? parsed : null;
  } catch (err) {
    console.error('Video analysis error:', err.message);
    return null;
  }
}

function buildVideoEmbed(v, analysis, rank) {
  const creator = v.tiktok_account ? `@${v.tiktok_account}` : 'Creator';
  const er = engagementRate(v);

  const embed = new EmbedBuilder()
    .setTitle(`#${rank} — ${creator}`)
    .setURL(v.video_url)
    .setColor(0x10B981)
    .addFields(
      { name: '💰 Revenue', value: formatMoney(v.gmv), inline: true },
      { name: '👀 Views', value: formatViews(v.views), inline: true },
      { name: '❤️ Engagement', value: er !== null ? `${er}%` : '—', inline: true },
    );

  if (analysis) {
    if (analysis.whyItWorked) embed.addFields({ name: '✨ Why It Worked', value: String(analysis.whyItWorked).substring(0, 1024) });
    if (analysis.format) embed.addFields({ name: '🎬 Format', value: String(analysis.format).substring(0, 1024) });
    if (analysis.hook) embed.addFields({ name: '🪝 Hook', value: String(analysis.hook).substring(0, 1024) });
    if (analysis.emotion) embed.addFields({ name: '🫀 Emotion', value: String(analysis.emotion).substring(0, 1024) });
    if (analysis.painPoint) embed.addFields({ name: '🎯 Pain Point', value: String(analysis.painPoint).substring(0, 1024) });
    if (analysis.difficulty) embed.addFields({ name: '⚡ Difficulty', value: String(analysis.difficulty), inline: true });
    if (analysis.score) embed.addFields({ name: '📊 Score', value: `${analysis.score}/10`, inline: true });
    if (analysis.howToAdapt) embed.addFields({ name: '🚀 How To Adapt This', value: String(analysis.howToAdapt).substring(0, 1024) });
  }

  embed.setFooter({ text: 'Study it, adapt it, ship it.' });
  return embed;
}

// Cache built content per guild so /inspo can't rack up API cost
const inspoCache = new Map(); // guildId → { at, intro, embeds }
const INSPO_CACHE_MS = 6 * 3600000;

async function buildInspoContent(guild, { force = false } = {}) {
  const cached = inspoCache.get(guild.id);
  if (!force && cached && Date.now() - cached.at < INSPO_CACHE_MS) return cached;

  const { videos, windowDays } = await getTopVideosForServer(guild.id);
  if (!videos.length) return null;

  const brandCtx = getBrandContext(guild.id);
  const brandName = brandCtx.brandName || guild.name;

  const analyses = (await analyzeTopVideos(videos, brandName)) || [];
  const embeds = videos.map((v, i) => buildVideoEmbed(v, analyses[i], i + 1));
  const intro = `**Top ${videos.length} performing creator videos from the last ${windowDays} days** 📈\nReal videos. Real GMV. Real analysis. Study the breakdowns below, adapt what works, ship your version. Type \`/inspo\` any time for the latest.`;

  const content = { at: Date.now(), intro, embeds };
  inspoCache.set(guild.id, content);
  return content;
}

async function postContentInspiration(guild, targetChannel, { force = false } = {}) {
  const content = await buildInspoContent(guild, { force });
  if (!content) return false;

  const brandCtx = getBrandContext(guild.id);
  const brandName = brandCtx.brandName || guild.name;

  if (brandCtx.conversational) {
    // Conversational servers — post directly as the bot
    await targetChannel.send(content.intro);
    for (const embed of content.embeds) await targetChannel.send({ embeds: [embed] });
  } else {
    // Brand servers — post via webhook as the brand
    const webhook = await getOrCreateWebhook(targetChannel, brandName);
    const send = webhook
      ? (payload) => webhook.send({ ...payload, username: brandName, avatarURL: guild.iconURL({ size: 128 }) })
      : (payload) => targetChannel.send(payload);
    await send({ content: content.intro });
    for (const embed of content.embeds) await send({ embeds: [embed] });
  }

  console.log(`Content inspiration posted in #${targetChannel.name} (${guild.name}) — ${content.embeds.length} videos`);
  return true;
}

async function handleInspireCommand(message) {
  if (message.content !== '!inspire') return false;
  if (!isTeamMember(message.member)) return false;

  if (!hubSupabase) {
    await message.reply('Hub database not connected — set HUB_SUPABASE_URL and HUB_SUPABASE_KEY env vars.');
    return true;
  }

  const posted = await postContentInspiration(message.guild, message.channel, { force: true });
  if (!posted) {
    await message.reply('No video performance data available for this brand yet. Make sure Euka sync is running in the Hub.');
  }

  try { await message.delete(); } catch (_) {}
  return true;
}

// Scheduled content inspiration (Tuesday & Thursday at 10:00)
async function postWeeklyInspiration() {
  if (!hubSupabase) return;

  for (const [, guild] of client.guilds.cache) {
    try {
      // Find the best channel to post in (content-inspiration, announcements, or general)
      const textChannels = guild.channels.cache.filter(c => c.type === 0);
      const target = textChannels.find(c => c.name.includes('content') || c.name.includes('inspiration'))
        || textChannels.find(c => c.name.includes('announcement'))
        || textChannels.find(c => c.name.includes('general') || c.name.includes('chat'));

      if (!target) continue;

      await postContentInspiration(guild, target, { force: true });
    } catch (err) {
      console.error(`Scheduled inspiration error for ${guild.name}:`, err.message);
    }
  }
}

// ── Daily Metrics Snapshot (all servers) ─────────────────────────────────────
async function takeMetricsSnapshot() {
  const today = new Date().toISOString().split('T')[0];
  const todayStart = new Date(today + 'T00:00:00Z').toISOString();
  const todayEnd = new Date(today + 'T23:59:59Z').toISOString();

  for (const [, guild] of client.guilds.cache) {
    const serverId = guild.id;

    // ── Messages today ──
    const { data: todayMessages } = await supabase.from('discord_messages')
      .select('author_id, author_name, is_team_member, is_answered, channel_name, replied_by')
      .eq('discord_server_id', serverId)
      .gte('created_at', todayStart).lte('created_at', todayEnd);

    const totalMsgs = todayMessages?.length || 0;
    const uniqueAuthors = new Set(todayMessages?.map(m => m.author_id) || []);
    const memberMessages = todayMessages?.filter(m => !m.is_team_member) || [];

    // ── Response times ──
    const { data: responseTimes } = await supabase.from('discord_messages')
      .select('reply_time_seconds, replied_by')
      .eq('discord_server_id', serverId).eq('is_answered', true)
      .not('reply_time_seconds', 'is', null)
      .gte('created_at', todayStart).lte('created_at', todayEnd);

    const avgResponseTime = responseTimes?.length
      ? Math.round(responseTimes.reduce((a, b) => a + b.reply_time_seconds, 0) / responseTimes.length) : null;

    // ── DAU / WAU / MAU ──
    const weekAgo = new Date(Date.now() - 7 * 86400000).toISOString();
    const { data: weekMsgs } = await supabase.from('discord_messages').select('author_id')
      .eq('discord_server_id', serverId).gte('created_at', weekAgo);
    const wau = new Set(weekMsgs?.map(m => m.author_id) || []).size;

    const monthAgo = new Date(Date.now() - 30 * 86400000).toISOString();
    const { data: monthMsgs } = await supabase.from('discord_messages').select('author_id')
      .eq('discord_server_id', serverId).gte('created_at', monthAgo);
    const mau = new Set(monthMsgs?.map(m => m.author_id) || []).size;

    const dau = dailyActiveUsers.get(serverId)?.size || uniqueAuthors.size;
    const stickiness = mau > 0 ? Math.round((dau / mau) * 10000) / 100 : 0;

    // ── Member growth (joins/leaves today) ──
    const { data: joinEvents } = await supabase.from('discord_member_events')
      .select('user_id').eq('discord_server_id', serverId).eq('event_type', 'join')
      .gte('created_at', todayStart).lte('created_at', todayEnd);
    const { data: leaveEvents } = await supabase.from('discord_member_events')
      .select('user_id').eq('discord_server_id', serverId).eq('event_type', 'leave')
      .gte('created_at', todayStart).lte('created_at', todayEnd);
    const newJoins = joinEvents?.length || 0;
    const leaves = leaveEvents?.length || 0;

    // ── Per-channel activity ──
    const channelActivity = {};
    for (const msg of (todayMessages || [])) {
      const ch = msg.channel_name || 'unknown';
      channelActivity[ch] = (channelActivity[ch] || 0) + 1;
    }

    // ── Top contributors (non-team, top 10 by message count today) ──
    const authorCounts = {};
    for (const msg of (memberMessages || [])) {
      const name = msg.author_name || msg.author_id;
      authorCounts[name] = (authorCounts[name] || 0) + 1;
    }
    const topContributors = Object.entries(authorCounts)
      .sort((a, b) => b[1] - a[1])
      .slice(0, 10)
      .map(([name, count]) => ({ name, count }));

    // ── Cohort retention (monthly cohorts based on first message date) ──
    let cohortData = null;
    try {
      // Get all messages for this server to build cohorts
      const { data: allMsgs } = await supabase.from('discord_messages')
        .select('author_id, created_at')
        .eq('discord_server_id', serverId)
        .eq('is_team_member', false)
        .order('created_at', { ascending: true });

      if (allMsgs?.length) {
        // Find each user's first message month (cohort) and all active months
        const userFirstMonth = {};
        const userActiveMonths = {};
        for (const msg of allMsgs) {
          const month = msg.created_at.substring(0, 7); // YYYY-MM
          if (!userFirstMonth[msg.author_id]) userFirstMonth[msg.author_id] = month;
          if (!userActiveMonths[msg.author_id]) userActiveMonths[msg.author_id] = new Set();
          userActiveMonths[msg.author_id].add(month);
        }

        // Build cohort retention table
        const cohorts = {};
        for (const [userId, firstMonth] of Object.entries(userFirstMonth)) {
          if (!cohorts[firstMonth]) cohorts[firstMonth] = { count: 0, retention: {} };
          cohorts[firstMonth].count++;
          for (const activeMonth of userActiveMonths[userId]) {
            if (!cohorts[firstMonth].retention[activeMonth]) cohorts[firstMonth].retention[activeMonth] = 0;
            cohorts[firstMonth].retention[activeMonth]++;
          }
        }

        // Convert to offset-based format (M0, M1, M2...)
        const sortedCohortMonths = Object.keys(cohorts).sort();
        const allMonths = [...new Set(allMsgs.map(m => m.created_at.substring(0, 7)))].sort();

        cohortData = sortedCohortMonths.map(cohortMonth => {
          const c = cohorts[cohortMonth];
          const cohortIdx = allMonths.indexOf(cohortMonth);
          const retention = {};
          for (const [activeMonth, activeCount] of Object.entries(c.retention)) {
            const offset = allMonths.indexOf(activeMonth) - cohortIdx;
            if (offset >= 0) {
              retention[`M${offset}`] = Math.round((activeCount / c.count) * 100);
            }
          }
          return { cohortMonth, count: c.count, retention };
        });
      }
    } catch (err) {
      console.error('Cohort calc error:', err.message);
    }

    // ── Upsert snapshot ──
    await supabase.from('discord_community_metrics').upsert({
      discord_server_id: serverId,
      server_name: guild.name,
      snapshot_date: today,
      total_members: guild.memberCount,
      online_members: guild.approximatePresenceCount || 0,
      new_joins: newJoins,
      leaves,
      net_growth: newJoins - leaves,
      total_messages: totalMsgs,
      unique_messagers: uniqueAuthors.size,
      messages_from_members: memberMessages.length,
      messages_answered: memberMessages.filter(m => m.is_answered).length,
      messages_unanswered: memberMessages.filter(m => !m.is_answered).length,
      avg_response_time_seconds: avgResponseTime,
      dau, wau, mau,
      stickiness_ratio: stickiness,
      channel_activity: channelActivity,
      top_contributors: topContributors,
      cohort_data: cohortData,
    }, { onConflict: 'discord_server_id,snapshot_date' });

    console.log(`Snapshot: ${guild.name} — ${totalMsgs} msgs, ${dau} DAU, ${mau} MAU, +${newJoins}/-${leaves} members`);
    dailyActiveUsers.set(serverId, new Set());
  }
}

// ── Daily Digest (all servers) ──────────────────────────────────────────────
async function postDailyDigest() {
  const yesterday = new Date(Date.now() - 86400000).toISOString().split('T')[0];

  for (const [, guild] of client.guilds.cache) {
    const { data: metrics } = await supabase.from('discord_community_metrics')
      .select('*').eq('discord_server_id', guild.id).eq('snapshot_date', yesterday).single();
    if (!metrics) continue;

    const responseRate = metrics.messages_from_members > 0
      ? Math.round((metrics.messages_answered / metrics.messages_from_members) * 100) : 100;
    const avgMins = metrics.avg_response_time_seconds
      ? Math.round(metrics.avg_response_time_seconds / 60) : 'N/A';

    // Growth stats
    const growthSign = metrics.net_growth >= 0 ? '+' : '';
    const growthText = `+${metrics.new_joins || 0} joined / -${metrics.leaves || 0} left (net: ${growthSign}${metrics.net_growth || 0})`;

    // Top contributors
    const topContribs = metrics.top_contributors || [];
    const topText = topContribs.length > 0
      ? topContribs.slice(0, 3).map((c, i) => `${i + 1}. ${c.name} (${c.count} msgs)`).join('\n')
      : 'No community messages';

    // Cohort highlight
    let cohortHighlight = '';
    if (metrics.cohort_data?.length) {
      const latest = metrics.cohort_data[metrics.cohort_data.length - 1];
      const olderCohorts = metrics.cohort_data.filter(c => Object.keys(c.retention).length > 1);
      if (olderCohorts.length > 0) {
        const c = olderCohorts[olderCohorts.length - 1];
        const offsets = Object.keys(c.retention).filter(k => k !== 'M0').sort();
        const lastOffset = offsets[offsets.length - 1];
        if (lastOffset) {
          cohortHighlight = `${c.cohortMonth} cohort (${c.count} creators): ${c.retention[lastOffset]}% still active at ${lastOffset}`;
        }
      }
    }

    // Discord embed
    const alertChannel = await getOrCreateAlertChannel(guild);
    if (alertChannel) {
      const embed = new EmbedBuilder()
        .setTitle(`Daily Community Digest — ${yesterday}`)
        .setColor(responseRate >= 90 ? 0x00CC66 : responseRate >= 70 ? 0xFFAA00 : 0xFF4444)
        .addFields(
          { name: 'Members', value: `${metrics.total_members} total`, inline: true },
          { name: 'Growth', value: growthText, inline: true },
          { name: 'Messages', value: `${metrics.total_messages} (${metrics.unique_messagers} people)`, inline: true },
          { name: 'Response Rate', value: `${responseRate}%`, inline: true },
          { name: 'Avg Response Time', value: `${avgMins} min`, inline: true },
          { name: 'Unanswered', value: `${metrics.messages_unanswered}`, inline: true },
          { name: 'DAU / WAU / MAU', value: `${metrics.dau} / ${metrics.wau} / ${metrics.mau}`, inline: true },
          { name: 'Stickiness', value: `${metrics.stickiness_ratio}%`, inline: true },
          { name: 'Top Contributors', value: topText, inline: false },
        ).setTimestamp();

      if (cohortHighlight) {
        embed.addFields({ name: 'Retention', value: cohortHighlight });
      }

      if (metrics.messages_unanswered > 0) {
        const { data: unanswered } = await supabase.from('discord_messages')
          .select('channel_name, author_name, content_preview')
          .eq('discord_server_id', guild.id).eq('is_answered', false).eq('is_team_member', false)
          .gte('created_at', yesterday + 'T00:00:00Z').lte('created_at', yesterday + 'T23:59:59Z').limit(5);
        if (unanswered?.length) {
          embed.addFields({ name: 'Still Unanswered', value: unanswered.map(m =>
            `• **#${m.channel_name}** — ${m.author_name}: ${m.content_preview}`).join('\n') });
        }
      }
      await alertChannel.send({ embeds: [embed] });
    }

    // Slack digest
    const emoji = responseRate >= 90 ? ':large_green_circle:' : responseRate >= 70 ? ':large_orange_circle:' : ':red_circle:';
    const slackBlocks = [
      { type: 'header', text: { type: 'plain_text', text: `${guild.name} Community Digest — ${yesterday}` } },
      { type: 'section', fields: [
        { type: 'mrkdwn', text: `*Members:* ${metrics.total_members}` },
        { type: 'mrkdwn', text: `*Growth:* ${growthText}` },
        { type: 'mrkdwn', text: `*Messages:* ${metrics.total_messages} (${metrics.unique_messagers} people)` },
        { type: 'mrkdwn', text: `*Response Rate:* ${emoji} ${responseRate}%` },
        { type: 'mrkdwn', text: `*Avg Response:* ${avgMins} min` },
        { type: 'mrkdwn', text: `*Unanswered:* ${metrics.messages_unanswered}` },
        { type: 'mrkdwn', text: `*DAU / WAU / MAU:* ${metrics.dau} / ${metrics.wau} / ${metrics.mau}` },
        { type: 'mrkdwn', text: `*Stickiness:* ${metrics.stickiness_ratio}%` },
      ]},
      { type: 'section', text: { type: 'mrkdwn', text: `*Top Contributors:*\n${topText}` } },
    ];
    if (cohortHighlight) {
      slackBlocks.push({ type: 'section', text: { type: 'mrkdwn', text: `:chart_with_upwards_trend: *Retention:* ${cohortHighlight}` } });
    }
    if (metrics.messages_unanswered > 0) {
      slackBlocks.push({ type: 'section', text: { type: 'mrkdwn',
        text: `:warning: *${metrics.messages_unanswered} message(s) still unanswered from yesterday*` } });
    }
    await slackAlert(`${guild.name} Community Digest — ${yesterday}`, slackBlocks);

    console.log(`Digest posted for ${guild.name}`);
  }
}

// ── AI Auto-Reply ───────────────────────────────────────────────────────────
function getBrandContext(serverId) {
  return { ...defaultBotConfig, ...(botConfigCache.get(serverId) || {}) };
}

function isAutoReplyRateLimited(serverId) {
  const limit = autoReplyLimits.get(serverId);
  const maxPerHour = getBrandContext(serverId).maxRepliesPerHour || 10;
  if (!limit || Date.now() > limit.resetTime) {
    autoReplyLimits.set(serverId, { count: 0, resetTime: Date.now() + 3600000 });
    return false;
  }
  return limit.count >= maxPerHour;
}

function incrementAutoReplyCount(serverId) {
  const limit = autoReplyLimits.get(serverId);
  if (limit) limit.count++;
}

async function generateAutoReply(message, brandCtx) {
  if (!anthropic) return null;

  const faqEntries = brandCtx.faq
    ? Object.entries(brandCtx.faq).map(([q, a]) => `Q: ${q}\nA: ${a}`).join('\n\n')
    : '';

  const contextInfo = (brandCtx.context || []).join('\n');

  const isConversational = brandCtx.conversational === true;

  // ── Fetch conversation context + member history in parallel ──
  const [recentMsgs, authorHistoryResult] = await Promise.all([
    message.channel.messages.fetch({ limit: 8, before: message.id }).catch(() => new Map()),
    supabase.from('discord_messages')
      .select('content_preview, channel_name, created_at')
      .eq('discord_server_id', message.guild.id)
      .eq('author_id', message.author.id)
      .order('created_at', { ascending: false })
      .limit(5),
  ]);

  // Build conversation context (recent messages in channel)
  let conversationContext = '';
  if (recentMsgs.size > 0) {
    const sorted = [...recentMsgs.values()]
      .sort((a, b) => a.createdTimestamp - b.createdTimestamp)
      .map(m => `${m.author.displayName || m.author.username}: ${m.content || '(attachment)'}`)
      .join('\n');
    conversationContext = `\n\nRecent conversation in this channel:\n${sorted}`;
  }

  // Build member memory context
  let memberMemory = '';
  const authorHistory = authorHistoryResult?.data || [];
  if (authorHistory.length > 1) {
    memberMemory = `\n\n[Note: You've chatted with ${message.author.displayName || message.author.username} before — they're a returning member, not new.]`;
  } else if (authorHistory.length === 0) {
    memberMemory = `\n\n[Note: This looks like ${message.author.displayName || message.author.username}'s first message in this server.]`;
  }

  // Media awareness
  let mediaInfo = '';
  if (message.attachments?.size > 0) {
    const types = [...message.attachments.values()].map(a => {
      if (a.contentType?.startsWith('image/')) return 'an image';
      if (a.contentType?.startsWith('video/')) return 'a video';
      return 'a file';
    });
    mediaInfo = `\n[They also shared ${types.join(' and ')}]`;
  }
  if (!mediaInfo && message.embeds?.length > 0) {
    const embedTypes = message.embeds.map(e => {
      if (e.video) return 'a video link';
      if (e.url?.includes('tiktok')) return 'a TikTok link';
      if (e.image) return 'an image link';
      return 'a link';
    });
    if (embedTypes.length) mediaInfo = `\n[They also shared ${embedTypes.join(' and ')}]`;
  }

  const questionOnlyRules = `Rules:
- Reply in 1-3 sentences max. Be helpful and concise.
- Never say you're an AI, bot, or assistant. You ARE the brand team.
- If the message is just chit-chat, a greeting, or doesn't need a response, reply with exactly: NO_REPLY
- If you're not confident you can give an accurate, helpful answer, reply with exactly: NO_REPLY
- Do not make up information. Only use what's in the brand context and FAQ above.
- Match the casual, friendly tone of Discord. Use emojis naturally but sparingly.
- If someone asks something not covered in FAQ/context, reply with: NO_REPLY`;

  const conversationalRules = `You MUST reply to every message. Never reply with NO_REPLY unless it's literal spam.

You are Alice. You work at Social Tale. You're chatting in a Discord group. Sound like a real 20-something woman typing quickly in a group chat, not a customer service bot.

CRITICAL STYLE RULES:
- 1-2 sentences MAX. Seriously, keep it short. Real people don't write paragraphs in Discord.
- NEVER use em dashes. Ever.
- NEVER start with "Hey [name]!" every time. Mix it up. Sometimes just jump in with "oh nice", "love that", "omg", "wait that's cool" etc.
- NEVER use the phrase "are you a creator, brand partner, or..." that sounds like a chatbot menu.
- NEVER say "that's awesome" or "that's amazing" repeatedly.
- NEVER ask what "brings them here" or what they're "looking to explore". That's bot talk.
- Don't repeat the same reply structure. Vary it every time.
- Use normal capitalisation. Start sentences with a capital letter like a normal person.
- One emoji max per message, and skip emojis sometimes.
- Don't over-compliment. A simple "welcome!" or "glad you're here" is enough.
- When someone introduces themselves, pick ONE specific thing they mentioned and react to that naturally. Don't summarise their whole intro back at them.
- Ask short natural questions like "what kind of stuff do you post?" or "how long have you been on tiktok?" not formal questions.
- If someone asks about retainers, just say we're always running them and to check #brand-deals and #announcements.
- Match the energy. If someone writes one word, don't write four sentences back.
- If someone shares a video, image, or TikTok link, acknowledge it naturally. You can't see it but react to the fact they shared something.
- Read the recent conversation in the channel and don't repeat what others have already said. Jump into the flow naturally.
- If you recognise a returning member, don't treat them like a stranger. A casual "hey again" vibe is fine.
- Never say you're an AI or bot. You ARE Alice, part of the team.`;

  const tiktokKnowledge = isConversational ? `\n\nTikTok Shop Knowledge (use when relevant, don't dump info unprompted):\n${TIKTOK_KNOWLEDGE_TEXT}` : '';

  const systemPrompt = `You are ${isConversational ? 'Alice, a friendly team member at' : 'replying as'} ${brandCtx.brandName || 'the brand'} in a Discord community server${isConversational ? '' : ' for creators and affiliates'}.

Brand voice: ${brandCtx.brandVoice || defaultBotConfig.brandVoice}

Brand context:
${contextInfo}

Known FAQ:
${faqEntries}${tiktokKnowledge}

${isConversational ? conversationalRules : questionOnlyRules}`;

  // Include learned FAQ from team answers
  const learnedFAQText = isConversational ? getLearnedFAQText(message.guild.id) : '';

  // Build enhanced user message with all context
  const userContent = `Discord message from ${message.author.displayName || message.author.username} in #${message.channel.name}:\n\n${message.content}${mediaInfo}${conversationContext}${memberMemory}${learnedFAQText}`;

  try {
    const response = await anthropic.messages.create({
      model: isConversational ? 'claude-sonnet-4-6' : 'claude-haiku-4-5-20251001',
      max_tokens: 200,
      system: systemPrompt,
      messages: [{ role: 'user', content: userContent }],
    });

    const reply = response.content[0]?.text?.trim();
    if (!reply || reply.includes('NO_REPLY')) return null;
    return reply;
  } catch (err) {
    console.error('AI reply error:', err.message);
    return null;
  }
}

function scheduleAutoReply(message) {
  const brandCtx = getBrandContext(message.guild.id);
  if (!brandCtx.autoReplyEnabled) return;
  if (!anthropic) return;

  // Don't reply in excluded channels
  const excludeChannels = brandCtx.excludeChannels || defaultBotConfig.excludeChannels || [];
  const channelName = message.channel.name.replace(/[^\w-]/g, ''); // strip emojis for matching
  if (excludeChannels.some(ex => message.channel.name.includes(ex) || channelName.includes(ex))) return;

  // Busy channel cooldown — only for brand servers (non-conversational)
  // Conversational servers (Social Tale) skip this — Alice is the sole community manager
  if (!brandCtx.conversational) {
    const recentInChannel = message.channel.messages.cache.filter(m =>
      Date.now() - m.createdTimestamp < 5 * 60 * 1000 && !m.author.bot
    ).size;
    if (recentInChannel > 5 && Math.random() < 0.7) {
      console.log(`Skipping reply in busy #${message.channel.name} (${recentInChannel} msgs in 5min)`);
      return;
    }
  }

  // Variable timing — randomize delay ±30% so replies don't feel robotic
  const baseDelayMs = (brandCtx.autoReplyDelayMinutes || defaultBotConfig.autoReplyDelayMinutes || 15) * 60 * 1000;
  const jitter = (Math.random() - 0.5) * baseDelayMs * 0.6;
  const delayMs = Math.max(60000, Math.round(baseDelayMs + jitter));

  const timeoutId = setTimeout(async () => {
    pendingAutoReplies.delete(message.id);

    // Check if already answered by team
    const serverPending = pendingMessages.get(message.guild.id);
    if (!serverPending?.has(message.id)) return; // already answered

    // Rate limit check
    if (isAutoReplyRateLimited(message.guild.id)) return;

    // Smart mixing: after consecutive replies in same channel, sometimes just react
    const streak = channelReplyStreak.get(message.channel.id) || 0;
    const reactOnly = brandCtx.conversational && streak >= 2 && Math.random() < 0.6;

    if (reactOnly) {
      // React-only — add emoji, skip text reply
      try {
        const emoji = pickReaction(message.content);
        await message.react(emoji);
        console.log(`React-only in #${message.channel.name}: ${emoji} on "${message.content.substring(0, 40)}"`);
      } catch (e) { console.error('React error:', e.message); }
      channelReplyStreak.set(message.channel.id, 0); // reset streak
      // Mark as answered
      if (serverPending) serverPending.delete(message.id);
      await supabase.from('discord_messages')
        .update({
          is_answered: true,
          replied_at: new Date().toISOString(),
          replied_by: 'auto-react',
          reply_time_seconds: Math.floor(delayMs / 1000),
        })
        .eq('discord_message_id', message.id)
        .eq('discord_server_id', message.guild.id);
      return;
    }

    // Generate reply
    const reply = await generateAutoReply(message, brandCtx);
    if (!reply) return;

    // Strip em dashes
    let cleanReply = reply.replace(/—/g, ',').replace(/–/g, ',');

    // In conversational servers, reply directly as Alice. In brand servers, use webhook.
    if (brandCtx.conversational) {
      await message.reply(cleanReply);
      // Sometimes also add a reaction alongside the reply (30% chance)
      if (Math.random() < 0.3) {
        try { await message.react(pickReaction(message.content)); } catch (_) {}
      }
    } else {
      const mention = `<@${message.author.id}>`;
      const webhook = await getOrCreateWebhook(message.channel, brandCtx.brandName || message.guild.name);
      if (webhook) {
        await webhook.send({
          content: `${mention} ${cleanReply}`,
          username: brandCtx.brandName || message.guild.name,
          avatarURL: message.guild.iconURL({ size: 128 }),
        });
      } else {
        await message.reply(cleanReply);
      }
    }

    channelReplyStreak.set(message.channel.id, streak + 1);
    incrementAutoReplyCount(message.guild.id);

    // Mark as answered
    if (serverPending) serverPending.delete(message.id);
    await supabase.from('discord_messages')
      .update({
        is_answered: true,
        replied_at: new Date().toISOString(),
        replied_by: 'auto-reply',
        reply_time_seconds: Math.floor(delayMs / 1000),
      })
      .eq('discord_message_id', message.id)
      .eq('discord_server_id', message.guild.id);

    console.log(`Auto-replied in #${message.channel.name} (${message.guild.name}): ${cleanReply.substring(0, 80)}...`);

    // Notify Slack that bot auto-replied
    await slackAlert(`Auto-replied in ${message.guild.name} Discord`, [
      { type: 'section', text: { type: 'mrkdwn',
        text: `*#${message.channel.name}* — ${message.author.username} asked:\n>${message.content.substring(0, 200)}\n\n*Bot replied:*\n${cleanReply}` } },
      { type: 'context', elements: [{ type: 'mrkdwn', text: ':robot_face: Auto-reply after ' + (delayMs / 60000) + 'min — check if response is accurate' }] },
    ]);
  }, delayMs);

  pendingAutoReplies.set(message.id, timeoutId);
}

function cancelAutoReply(messageId) {
  const timeoutId = pendingAutoReplies.get(messageId);
  if (timeoutId) {
    clearTimeout(timeoutId);
    pendingAutoReplies.delete(messageId);
  }
}

// ── Spam / Bad Actor Detection ───────────────────────────────────────────────
const spamTracker = new Map(); // authorId → { count, firstSeen, reasons }

const SPAM_LINK_PATTERNS = [
  /discord\.gg\/\w+/i,               // Discord invite links
  /bit\.ly\/\w+/i,                    // Shortened URLs
  /t\.me\/\w+/i,                      // Telegram links
  /wa\.me\/\w+/i,                     // WhatsApp links
  /free\s*(nitro|gift|steam|crypto)/i, // Free nitro/gift scams
  /claim\s*(your|free|now)/i,         // Claim scams
  /(earn|make)\s*\$?\d+.*daily/i,     // Money scams
  /onlyfans\.com/i,                   // NSFW spam
  /crypto.*airdrop/i,                 // Crypto scams
  /click\s*(here|this|the\s*link)/i,  // Generic phishing
  /@everyone.*http/i,                 // @everyone with link
];

const SPAM_WORD_PATTERNS = [
  /\b(buy followers|buy likes|get rich quick)\b/i,
  /\b(dm me for|hmu for|check my bio)\b/i,
  /\b(nudes|18\+|xxx|porn)\b/i,
  /\b(wire transfer|western union|cash ?app.*send)\b/i,
];

function detectSpam(message) {
  const content = message.content || '';
  const authorId = message.author.id;

  // Check for spam link patterns
  for (const pattern of SPAM_LINK_PATTERNS) {
    if (pattern.test(content)) {
      trackSpamOffence(authorId, 'suspicious link');
      const tracker = spamTracker.get(authorId);
      return { reason: `Suspicious link detected: ${pattern}`, kicked: tracker && tracker.count >= 2 };
    }
  }

  // Check for spam word patterns
  for (const pattern of SPAM_WORD_PATTERNS) {
    if (pattern.test(content)) {
      trackSpamOffence(authorId, 'spam content');
      const tracker = spamTracker.get(authorId);
      return { reason: `Spam content detected: ${pattern}`, kicked: tracker && tracker.count >= 2 };
    }
  }

  // Mass mentions (3+ user mentions in one message)
  const mentionCount = (content.match(/<@!?\d+>/g) || []).length;
  if (mentionCount >= 3) {
    trackSpamOffence(authorId, 'mass mentions');
    const tracker = spamTracker.get(authorId);
    return { reason: `Mass mentions (${mentionCount} users)`, kicked: tracker && tracker.count >= 2 };
  }

  // Rapid-fire messages (5+ messages in 10 seconds)
  const cached = message.channel.messages.cache.filter(m =>
    m.author.id === authorId && Date.now() - m.createdTimestamp < 10000
  );
  if (cached.size >= 5) {
    trackSpamOffence(authorId, 'message flooding');
    const tracker = spamTracker.get(authorId);
    return { reason: `Message flooding (${cached.size} messages in 10s)`, kicked: tracker && tracker.count >= 3 };
  }

  return null; // not spam
}

function trackSpamOffence(authorId, reason) {
  const existing = spamTracker.get(authorId);
  if (existing) {
    existing.count++;
    existing.reasons.push(reason);
  } else {
    spamTracker.set(authorId, { count: 1, firstSeen: Date.now(), reasons: [reason] });
  }
  // Clean up old entries every hour
  if (spamTracker.size > 100) {
    const hourAgo = Date.now() - 3600000;
    for (const [id, data] of spamTracker) {
      if (data.firstSeen < hourAgo) spamTracker.delete(id);
    }
  }
}

// ── Scheduled Engagement Posts (conversational servers only) ──────────────────
const ENGAGEMENT_SCHEDULE = [
  // { day: 0-6 (Sun-Sat), hour: 0-23 (UTC), type: prompt category }
  { day: 1, hour: 10, type: 'monday_checkin' },    // Monday 10am — weekend recap
  { day: 2, hour: 14, type: 'conversation' },       // Tuesday 2pm — general convo
  { day: 3, hour: 11, type: 'tiktok_topic' },       // Wednesday 11am — TikTok Shop topic
  { day: 4, hour: 15, type: 'conversation' },       // Thursday 3pm — general convo
  { day: 5, hour: 10, type: 'friday_weekend' },     // Friday 10am — weekend plans
];

const ENGAGEMENT_PROMPTS = {
  monday_checkin: [
    "Hope everyone had a great weekend! Anyone do anything fun? I spent mine catching up on TikTok trends 👀",
    "Happy Monday! How was everyone's weekend? Did anyone post any content over the weekend?",
    "Monday check-in! How's everyone feeling about the week ahead? Anyone got big content plans?",
    "Hey everyone! Hope the weekend treated you well — who's feeling motivated to crush it this week?",
    "Monday vibes! Anyone try any new content ideas over the weekend? Would love to hear what you've been working on",
  ],
  friday_weekend: [
    "Happy Friday! Who's planning to batch some content this weekend? What brands are you working on?",
    "It's Friday! Anyone going live this weekend? Drop your plans below 👇",
    "Weekend's almost here! What's everyone's content plan? Batch filming, going live, or taking a well-earned break?",
    "TGIF! Who's got posting plans this weekend? Always love seeing what everyone's working on",
    "Friday check-in! What was your biggest win this week? And who's creating content this weekend?",
  ],
  tiktok_topic: [
    "Quick question — what's been your best-performing video format lately? Short and snappy or longer storytelling?",
    "Been seeing a lot of creators crush it with behind-the-scenes content lately. Is anyone here doing BTS style videos?",
    "What's one thing you've learned about TikTok Shop that you wish you knew when you started?",
    "Live selling vs short-form videos — which one's been working better for you guys lately?",
    "Hot topic: what product categories do you think are about to blow up on TikTok Shop? I've got some thoughts but want to hear yours first 👀",
    "Anyone experimenting with different posting times? Curious what's been working for people",
    "What's your go-to hook for TikTok videos? The first 2 seconds make or break a video honestly",
    "Real talk — what's the hardest part about creating content for TikTok Shop? Let's problem-solve together",
  ],
  conversation: [
    "Random thought — what got you into content creation in the first place? Everyone's got a different story",
    "What's everyone watching/listening to at the moment? Always looking for new podcast or show recommendations",
    "If you could promote any brand on TikTok, dream brand with no limits, who would it be and why?",
    "Alright, fun one — what's the most unexpected product you've seen go viral on TikTok?",
    "How do you guys stay creative when you're not feeling it? Would love to hear everyone's tips",
    "What's your setup like for filming? Phone on a stack of books gang or full studio setup? No judgement either way 😂",
    "Anyone here juggling content creation with a full-time job or parenting? How do you manage your time?",
    "What's one goal you're working towards right now? Could be content-related or just life in general",
  ],
};

async function postEngagementMessage(type) {
  const prompts = ENGAGEMENT_PROMPTS[type];
  if (!prompts?.length) return;

  // Pick a random prompt from the category
  const message = prompts[Math.floor(Math.random() * prompts.length)];

  for (const [, guild] of client.guilds.cache) {
    const brandCtx = getBrandContext(guild.id);
    if (!brandCtx.conversational) continue; // only post in conversational servers

    // Find community-chat or general channel
    const textChannels = guild.channels.cache.filter(c => c.type === 0);
    const target = textChannels.find(c => c.name.includes('community') && c.name.includes('chat'))
      || textChannels.find(c => c.name.includes('general') && !c.name.includes('alert'))
      || textChannels.find(c => c.name.includes('chat') && !c.name.includes('alert') && !c.name.includes('team'));

    if (!target) continue;
    if (!target.permissionsFor(client.user)?.has(['ViewChannel', 'SendMessages'])) continue;

    try {
      // Post as Alice (directly, not via webhook — she's a community member)
      await target.send(message);
      console.log(`[Engagement] Posted ${type} in #${target.name} (${guild.name}): ${message.substring(0, 60)}...`);
    } catch (err) {
      console.error(`[Engagement] Error posting in ${guild.name}:`, err.message);
    }
  }
}

function scheduleEngagement() {
  for (const slot of ENGAGEMENT_SCHEDULE) {
    scheduleWeekly(slot.day, slot.hour, 0, () => postEngagementMessage(slot.type));
  }
  console.log(`Scheduled ${ENGAGEMENT_SCHEDULE.length} weekly engagement posts`);
}

// ── Sentiment Detection — fast-track alert for frustrated members ─────────────
const FRUSTRATED_PATTERNS = [
  /\b(frustrated|annoyed|pissed|angry|furious|upset|disappointed|fed up)\b/i,
  /\b(this is ridiculous|this is bs|what the hell|wtf|so annoying|sick of this)\b/i,
  /\b(no one (responds?|replies|helps?|answers?))\b/i,
  /\b(worst (experience|service|support))\b/i,
  /\b(been waiting (forever|weeks|days|ages))\b/i,
  /\b(waste of time|scam|rip ?off|don'?t bother)\b/i,
];

function detectSentiment(message) {
  const content = message.content || '';
  for (const pattern of FRUSTRATED_PATTERNS) {
    if (pattern.test(content)) return 'frustrated';
  }
  return null;
}

async function handleNegativeSentiment(message, sentiment) {
  console.log(`[Sentiment] ${sentiment} detected from ${message.author.username} in #${message.channel.name}`);
  await slackAlert(`:warning: Negative sentiment in ${message.guild.name}`, [
    { type: 'header', text: { type: 'plain_text', text: `${sentiment.toUpperCase()} member detected` } },
    { type: 'section', text: { type: 'mrkdwn',
      text: `*#${message.channel.name}* — *${message.author.username}*\n>${message.content.substring(0, 400)}` } },
    { type: 'section', text: { type: 'mrkdwn',
      text: `:point_right: <${message.url}|Jump to message> — someone should respond ASAP` } },
    { type: 'context', elements: [{ type: 'mrkdwn', text: ':brain: Automated sentiment detection — may be a false positive' }] },
  ]);
}

// ── Win Detection — celebrate achievements ───────────────────────────────────
const WIN_PATTERNS = [
  /\b(first (sale|order)|got my first)\b/i,
  /\b(went viral|blew up|over \d+k views)\b/i,
  /\b(hit \d+k? (followers|views|sales|orders))\b/i,
  /\b(made \$\d+|earned \$\d+)\b/i,
  /\b(best.selling|top seller|number one)\b/i,
  /\b(got approved|got accepted|got the retainer)\b/i,
  /\b(so excited|can't believe|omg.*sold)\b/i,
];

const WIN_REACTIONS = ['🔥', '🎉', '💪', '🙌', '🥳', '💯'];

function detectWin(message) {
  const content = message.content || '';
  for (const pattern of WIN_PATTERNS) {
    if (pattern.test(content)) return true;
  }
  return false;
}

async function celebrateWin(message) {
  try {
    // Add a celebration reaction
    const emoji = WIN_REACTIONS[Math.floor(Math.random() * WIN_REACTIONS.length)];
    await message.react(emoji);
    console.log(`[Win] Celebrated ${message.author.username}'s win in #${message.channel.name}`);
  } catch (err) {
    console.error('[Win] React error:', err.message);
  }
}

// ── FAQ Learning — track team answers for Alice to learn from ────────────────
const learnedFAQ = new Map(); // serverId → [{ question, answer, channel }]
const MAX_LEARNED_FAQ = 20; // keep last 20 per server

function trackTeamAnswer(teamMessage, originalQuestion) {
  if (!originalQuestion || !teamMessage.content) return;
  const serverId = teamMessage.guild.id;
  if (!learnedFAQ.has(serverId)) learnedFAQ.set(serverId, []);
  const faqs = learnedFAQ.get(serverId);
  faqs.push({
    question: originalQuestion.substring(0, 200),
    answer: teamMessage.content.substring(0, 300),
    channel: teamMessage.channel.name,
    timestamp: Date.now(),
  });
  // Keep only the most recent entries
  while (faqs.length > MAX_LEARNED_FAQ) faqs.shift();
}

function getLearnedFAQText(serverId) {
  const faqs = learnedFAQ.get(serverId);
  if (!faqs?.length) return '';
  return '\n\nRecent team answers (learn from these):\n' +
    faqs.slice(-10).map(f => `Q: ${f.question}\nA: ${f.answer}`).join('\n\n');
}

// ── Onboarding Drip — follow-up DMs on Day 3 and Day 7 ──────────────────────
async function runOnboardingDrip() {
  const now = Date.now();
  const day3Ago = new Date(now - 3 * 86400000).toISOString().split('T')[0];
  const day7Ago = new Date(now - 7 * 86400000).toISOString().split('T')[0];

  for (const [, guild] of client.guilds.cache) {
    const brandCtx = getBrandContext(guild.id);
    if (!brandCtx.conversational) continue;

    try {
      // Get members who joined 3 days ago
      const { data: day3Joins } = await supabase.from('discord_member_events')
        .select('user_id, username')
        .eq('discord_server_id', guild.id)
        .eq('event_type', 'join')
        .gte('created_at', day3Ago + 'T00:00:00Z')
        .lte('created_at', day3Ago + 'T23:59:59Z');

      for (const join of (day3Joins || [])) {
        // Check if they've posted anything
        const { data: messages } = await supabase.from('discord_messages')
          .select('discord_message_id')
          .eq('discord_server_id', guild.id)
          .eq('author_id', join.user_id)
          .limit(1);

        if (!messages?.length) {
          // They joined 3 days ago but haven't posted — send a nudge
          try {
            const member = await guild.members.fetch(join.user_id);
            await member.send(`Hey! Just checking in since you joined ${guild.name} a few days ago. If you haven't already, drop an intro in #intro-yourself and check out #brand-deals for current opportunities. Let me know if you have any questions!`);
            console.log(`[Onboarding] Day 3 nudge sent to ${join.username}`);
            await slackAlert(`Alice Day 3 nudge DM in ${guild.name}`, [
              { type: 'section', text: { type: 'mrkdwn', text: `*Member:* ${join.username}\n*Status:* Joined 3 days ago, hasn't posted yet` } },
            ]);
          } catch (_) {} // DMs might be disabled
        }
      }

      // Get members who joined 7 days ago
      const { data: day7Joins } = await supabase.from('discord_member_events')
        .select('user_id, username')
        .eq('discord_server_id', guild.id)
        .eq('event_type', 'join')
        .gte('created_at', day7Ago + 'T00:00:00Z')
        .lte('created_at', day7Ago + 'T23:59:59Z');

      for (const join of (day7Joins || [])) {
        const { data: messages } = await supabase.from('discord_messages')
          .select('discord_message_id')
          .eq('discord_server_id', guild.id)
          .eq('author_id', join.user_id)
          .limit(1);

        if (!messages?.length) {
          try {
            const member = await guild.members.fetch(join.user_id);
            await member.send(`Hey! It's been about a week since you joined ${guild.name}. Just wanted to make sure you're not missing out — we post creator opportunities and retainer deals in #brand-deals and #announcements regularly. Feel free to jump in anytime!`);
            console.log(`[Onboarding] Day 7 nudge sent to ${join.username}`);
            await slackAlert(`Alice Day 7 nudge DM in ${guild.name}`, [
              { type: 'section', text: { type: 'mrkdwn', text: `*Member:* ${join.username}\n*Status:* Joined 7 days ago, still hasn't posted` } },
            ]);
          } catch (_) {}
        }
      }
    } catch (err) {
      console.error(`[Onboarding] Error for ${guild.name}:`, err.message);
    }
  }
}

// ── Content Nudge — DM creators who go quiet for 2+ weeks ────────────────────
async function runContentNudges() {
  const twoWeeksAgo = new Date(Date.now() - 14 * 86400000).toISOString();
  const fourWeeksAgo = new Date(Date.now() - 28 * 86400000).toISOString();

  for (const [, guild] of client.guilds.cache) {
    const brandCtx = getBrandContext(guild.id);
    if (!brandCtx.conversational) continue;

    try {
      // Find users who were active in the last month but haven't posted in 2+ weeks
      const { data: recentlyActive } = await supabase.from('discord_messages')
        .select('author_id, author_name')
        .eq('discord_server_id', guild.id)
        .eq('is_team_member', false)
        .gte('created_at', fourWeeksAgo)
        .lte('created_at', twoWeeksAgo);

      if (!recentlyActive?.length) continue;

      // Deduplicate by author
      const uniqueAuthors = new Map();
      for (const msg of recentlyActive) {
        if (!uniqueAuthors.has(msg.author_id)) uniqueAuthors.set(msg.author_id, msg.author_name);
      }

      for (const [authorId, authorName] of uniqueAuthors) {
        // Check they haven't posted recently
        const { data: recent } = await supabase.from('discord_messages')
          .select('discord_message_id')
          .eq('discord_server_id', guild.id)
          .eq('author_id', authorId)
          .gte('created_at', twoWeeksAgo)
          .limit(1);

        if (recent?.length) continue; // still active

        // Check we haven't already nudged them (avoid spamming)
        const { data: recentNudge } = await supabase.from('discord_nudges')
          .select('id')
          .eq('discord_server_id', guild.id)
          .eq('user_id', authorId)
          .gte('created_at', fourWeeksAgo)
          .limit(1);
        if (recentNudge?.length) continue; // already nudged recently

        try {
          const member = await guild.members.fetch(authorId);
          const templates = [
            `Hey ${authorName}! Haven't seen you around in ${guild.name} for a bit. Hope everything's going well! We've got some new opportunities in #brand-deals if you're looking to get back into creating.`,
            `Hey! Just noticed you've been quiet in ${guild.name} lately. No pressure at all, just wanted to check in. There's some cool stuff happening in #announcements if you want to take a look!`,
          ];
          const dmText = templates[Math.floor(Math.random() * templates.length)];
          await member.send(dmText);
          console.log(`[Nudge] Sent content nudge to ${authorName}`);

          // Track the nudge to avoid repeat DMs
          await supabase.from('discord_nudges').insert({
            discord_server_id: guild.id,
            user_id: authorId,
            username: authorName,
            nudge_type: 'content_inactive',
          }).catch(() => {}); // table might not exist yet, that's ok

          await slackAlert(`Alice nudge DM in ${guild.name}`, [
            { type: 'section', text: { type: 'mrkdwn', text: `*Member:* ${authorName}\n*Reason:* Inactive for 2+ weeks\n*DM:* ${dmText}` } },
          ]);
        } catch (_) {} // member may have left or DMs disabled
      }
    } catch (err) {
      console.error(`[Nudge] Error for ${guild.name}:`, err.message);
    }
  }
}

// ── Auto-Role Assignment — reward active members ─────────────────────────────
const ROLE_THRESHOLDS = [
  { minMessages: 50, roleName: 'Top Contributor', color: '#FFD700' },
  { minMessages: 15, roleName: 'Active Creator', color: '#10B981' },
];

async function runAutoRoles() {
  const thirtyDaysAgo = new Date(Date.now() - 30 * 86400000).toISOString();

  for (const [, guild] of client.guilds.cache) {
    const brandCtx = getBrandContext(guild.id);
    if (!brandCtx.conversational) continue;
    if (!guild.members.me?.permissions.has('ManageRoles')) {
      console.log(`[AutoRole] Missing ManageRoles permission in ${guild.name}`);
      continue;
    }

    try {
      // Count messages per non-team author in last 30 days
      const { data: msgCounts } = await supabase.from('discord_messages')
        .select('author_id, author_name')
        .eq('discord_server_id', guild.id)
        .eq('is_team_member', false)
        .gte('created_at', thirtyDaysAgo);

      if (!msgCounts?.length) continue;

      const counts = {};
      for (const msg of msgCounts) {
        counts[msg.author_id] = (counts[msg.author_id] || 0) + 1;
      }

      for (const [authorId, count] of Object.entries(counts)) {
        // Find the highest threshold they qualify for
        const qualifiedRole = ROLE_THRESHOLDS.find(t => count >= t.minMessages);
        if (!qualifiedRole) continue;

        try {
          const member = await guild.members.fetch(authorId);
          // Check if they already have the role
          const existingRole = guild.roles.cache.find(r => r.name === qualifiedRole.roleName);
          if (existingRole && member.roles.cache.has(existingRole.id)) continue; // already has it

          // Create role if it doesn't exist
          let role = existingRole;
          if (!role) {
            role = await guild.roles.create({
              name: qualifiedRole.roleName,
              color: qualifiedRole.color,
              reason: 'Auto-role for active community members',
            });
            console.log(`[AutoRole] Created role "${qualifiedRole.roleName}" in ${guild.name}`);
          }

          // Assign the role
          await member.roles.add(role);
          console.log(`[AutoRole] Assigned "${qualifiedRole.roleName}" to ${member.user.username} (${count} msgs)`);
        } catch (err) {
          console.error(`[AutoRole] Error assigning to ${authorId}:`, err.message);
        }
      }
    } catch (err) {
      console.error(`[AutoRole] Error for ${guild.name}:`, err.message);
    }
  }
}

// ── Creator Spotlight — weekly shoutout to top creator ────────────────────────
async function postCreatorSpotlight() {
  if (!hubSupabase) return;

  const thirtyDaysAgo = new Date(Date.now() - 30 * 86400000).toISOString().split('T')[0];

  for (const [, guild] of client.guilds.cache) {
    const brandCtx = getBrandContext(guild.id);

    try {
      // Look up brand in Hub
      const { data: brand } = await hubSupabase
        .from('brands')
        .select('id, name')
        .eq('discord_server_id', guild.id)
        .single();
      if (!brand) continue;

      // Get top creator by views in last 30 days
      const { data: topCreators } = await hubSupabase
        .from('creator_videos')
        .select('tiktok_account, views, gmv, orders')
        .eq('brand_id', brand.id)
        .gte('post_date', thirtyDaysAgo)
        .order('views', { ascending: false })
        .limit(20);

      if (!topCreators?.length) continue;

      // Aggregate by creator
      const creatorStats = {};
      for (const v of topCreators) {
        if (!v.tiktok_account) continue;
        if (!creatorStats[v.tiktok_account]) {
          creatorStats[v.tiktok_account] = { views: 0, gmv: 0, orders: 0, videos: 0 };
        }
        const s = creatorStats[v.tiktok_account];
        s.views += v.views || 0;
        s.gmv += v.gmv || 0;
        s.orders += v.orders || 0;
        s.videos++;
      }

      // Find top creator
      const sorted = Object.entries(creatorStats).sort((a, b) => b[1].views - a[1].views);
      if (!sorted.length) continue;

      const [topHandle, stats] = sorted[0];

      // Find a suitable channel to post in
      const textChannels = guild.channels.cache.filter(c => c.type === 0);
      const target = textChannels.find(c => c.name.includes('community') && c.name.includes('chat'))
        || textChannels.find(c => c.name.includes('announcement'))
        || textChannels.find(c => c.name.includes('general'));
      if (!target) continue;

      const spotlightMsg = `Creator Spotlight this week goes to @${topHandle}! ${formatViews(stats.views)} total views across ${stats.videos} video${stats.videos > 1 ? 's' : ''} this month${stats.orders > 0 ? ` and ${stats.orders} orders` : ''}. Keep crushing it! 🔥`;

      if (brandCtx.conversational) {
        await target.send(spotlightMsg);
      } else {
        const webhook = await getOrCreateWebhook(target, brandCtx.brandName || guild.name);
        if (webhook) {
          await webhook.send({
            content: spotlightMsg,
            username: brandCtx.brandName || guild.name,
            avatarURL: guild.iconURL({ size: 128 }),
          });
        }
      }

      console.log(`[Spotlight] Posted creator spotlight for @${topHandle} in ${guild.name}`);
    } catch (err) {
      console.error(`[Spotlight] Error for ${guild.name}:`, err.message);
    }
  }
}

// ── Milestone Celebrations — detect first viral video, big sales, etc. ───────
async function checkMilestones() {
  if (!hubSupabase) return;

  const yesterday = new Date(Date.now() - 86400000).toISOString().split('T')[0];

  for (const [, guild] of client.guilds.cache) {
    const brandCtx = getBrandContext(guild.id);

    try {
      const { data: brand } = await hubSupabase
        .from('brands')
        .select('id, name')
        .eq('discord_server_id', guild.id)
        .single();
      if (!brand) continue;

      // Check for viral videos (10K+ views) posted yesterday
      const { data: viralVideos } = await hubSupabase
        .from('creator_videos')
        .select('tiktok_account, views, video_url')
        .eq('brand_id', brand.id)
        .eq('post_date', yesterday)
        .gte('views', 10000)
        .order('views', { ascending: false })
        .limit(5);

      if (!viralVideos?.length) continue;

      const textChannels = guild.channels.cache.filter(c => c.type === 0);
      const target = textChannels.find(c => c.name.includes('community') && c.name.includes('chat'))
        || textChannels.find(c => c.name.includes('announcement'))
        || textChannels.find(c => c.name.includes('general') || c.name.includes('chat'));
      if (!target) continue;

      for (const video of viralVideos) {
        const handle = video.tiktok_account ? `@${video.tiktok_account}` : 'One of our creators';
        const msg = `${handle} just hit ${formatViews(video.views)} views! 🎉${video.video_url ? ` Check it out: ${video.video_url}` : ''} Amazing work!`;

        if (brandCtx.conversational) {
          await target.send(msg);
        } else {
          const webhook = await getOrCreateWebhook(target, brandCtx.brandName || guild.name);
          if (webhook) {
            await webhook.send({ content: msg, username: brandCtx.brandName || guild.name, avatarURL: guild.iconURL({ size: 128 }) });
          }
        }
        console.log(`[Milestone] Celebrated ${handle} hitting ${formatViews(video.views)} views in ${guild.name}`);
      }
    } catch (err) {
      console.error(`[Milestone] Error for ${guild.name}:`, err.message);
    }
  }
}

// ── Daily Member Checks — runs once per day, handles drip + nudges + roles ───
async function dailyMemberChecks() {
  console.log('[Daily] Running member checks...');
  await runOnboardingDrip().catch(err => console.error('[Daily] Onboarding error:', err.message));
  await runContentNudges().catch(err => console.error('[Daily] Nudge error:', err.message));
  await runAutoRoles().catch(err => console.error('[Daily] AutoRole error:', err.message));
  await checkMilestones().catch(err => console.error('[Daily] Milestone error:', err.message));
  console.log('[Daily] Member checks complete');
}

// ── Startup Catchup — reply to recent unanswered messages ────────────────────
let isCatchingUp = false; // bypass rate limit during catchup

async function catchUpUnanswered() {
  isCatchingUp = true;
  const CATCHUP_HOURS = parseInt(process.env.CATCHUP_HOURS || '4', 10); // default 4h, override via env
  const CATCHUP_DELAY_MS = 5000; // 5s between replies to avoid flooding
  console.log(`[Catchup] Looking back ${CATCHUP_HOURS} hours`);
  const cutoff = new Date(Date.now() - CATCHUP_HOURS * 3600000);
  let totalCaughtUp = 0;

  for (const [, guild] of client.guilds.cache) {
    const brandCtx = getBrandContext(guild.id);
    if (!brandCtx.autoReplyEnabled) continue;

    const excludeChannels = brandCtx.excludeChannels || defaultBotConfig.excludeChannels || [];

    // Get text channels the bot can read
    const textChannels = guild.channels.cache.filter(c =>
      c.type === 0 &&
      c.permissionsFor(client.user)?.has(['ViewChannel', 'SendMessages', 'ReadMessageHistory']) &&
      !excludeChannels.some(ex => c.name.includes(ex))
    );

    console.log(`[Catchup] ${guild.name}: scanning ${textChannels.size} channels`);

    for (const [, channel] of textChannels) {
      try {
        // Fetch last 100 messages from this channel (more coverage for 7-day window)
        const messages = await channel.messages.fetch({ limit: 100 });
        const recent = messages.filter(m =>
          m.createdAt > cutoff &&
          !m.author.bot &&
          m.content?.trim().length > 0
        ).sort((a, b) => a.createdTimestamp - b.createdTimestamp);

        console.log(`[Catchup] #${channel.name}: ${messages.size} fetched, ${recent.size} in window`);

        for (const [, msg] of recent) {
          try {
            // Skip if from team
            let memberObj = msg.member;
            if (!memberObj) {
              try { memberObj = await guild.members.fetch(msg.author.id); } catch (_) {}
            }
            if (isTeamMember(memberObj)) {
              console.log(`[Catchup] Skipping team msg from ${msg.author.username}`);
              continue;
            }

            // Only skip if someone directly replied to this specific message
            const directReply = messages.find(m =>
              m.reference?.messageId === msg.id &&
              m.createdTimestamp > msg.createdTimestamp
            );
            if (directReply) {
              console.log(`[Catchup] Skipping already-replied msg from ${msg.author.username}: "${msg.content.substring(0, 40)}"`);
              continue;
            }

            // Check if already answered in DB
            const { data: existing } = await supabase.from('discord_messages')
              .select('is_answered')
              .eq('discord_message_id', msg.id)
              .eq('discord_server_id', guild.id)
              .single();
            if (existing?.is_answered) {
              console.log(`[Catchup] Skipping DB-answered msg from ${msg.author.username}`);
              continue;
            }

            // Rate limit check (skip during catchup)
            if (!isCatchingUp && isAutoReplyRateLimited(guild.id)) break;

            // Smart mixing during catchup: after 2+ consecutive replies, sometimes just react
            const cStreak = channelReplyStreak.get(channel.id) || 0;
            const reactOnlyCatchup = brandCtx.conversational && cStreak >= 2 && Math.random() < 0.6;

            if (reactOnlyCatchup) {
              try {
                const emoji = pickReaction(msg.content);
                await msg.react(emoji);
                console.log(`[Catchup] React-only in #${channel.name}: ${emoji} on "${msg.content.substring(0, 40)}"`);
              } catch (e) { console.error('[Catchup] React error:', e.message); }
              channelReplyStreak.set(channel.id, 0);
              await supabase.from('discord_messages').upsert({
                discord_message_id: msg.id,
                discord_server_id: guild.id,
                discord_channel_id: channel.id,
                channel_name: channel.name,
                author_id: msg.author.id,
                author_name: msg.author.displayName || msg.author.username,
                is_team_member: false,
                content_preview: msg.content?.substring(0, 100) || '(no text)',
                created_at: msg.createdAt.toISOString(),
                is_answered: true,
                replied_at: new Date().toISOString(),
                replied_by: 'auto-react-catchup',
                reply_time_seconds: Math.floor((Date.now() - msg.createdTimestamp) / 1000),
              }, { onConflict: 'discord_message_id,discord_server_id' });
              totalCaughtUp++;
              await new Promise(r => setTimeout(r, 2000)); // short delay for reacts
              continue;
            }

            // Generate and send reply
            console.log(`[Catchup] Generating reply for ${msg.author.username} in #${channel.name}: "${msg.content.substring(0, 60)}"`);
            const reply = await generateAutoReply(msg, brandCtx);
            if (!reply) {
              console.log(`[Catchup] AI returned NO_REPLY for ${msg.author.username}: "${msg.content.substring(0, 60)}"`);
              continue;
            }

            // Strip em dashes for conversational servers
            let cleanReply = reply.replace(/—/g, ',').replace(/–/g, ',');

            // In conversational servers, reply directly as Alice (not via webhook)
            if (brandCtx.conversational) {
              await msg.reply(cleanReply);
              // Sometimes also react alongside reply (30% chance)
              if (Math.random() < 0.3) {
                try { await msg.react(pickReaction(msg.content)); } catch (_) {}
              }
            } else {
              const mention = `<@${msg.author.id}>`;
              const webhook = await getOrCreateWebhook(channel, brandCtx.brandName || guild.name);
              if (webhook) {
                await webhook.send({
                  content: `${mention} ${cleanReply}`,
                  username: brandCtx.brandName || guild.name,
                  avatarURL: guild.iconURL({ size: 128 }),
                });
              } else {
                await msg.reply(cleanReply);
              }
            }

            channelReplyStreak.set(channel.id, cStreak + 1);
            incrementAutoReplyCount(guild.id);
            totalCaughtUp++;

            // Track in DB
            await supabase.from('discord_messages').upsert({
              discord_message_id: msg.id,
              discord_server_id: guild.id,
              discord_channel_id: channel.id,
              channel_name: channel.name,
              author_id: msg.author.id,
              author_name: msg.author.displayName || msg.author.username,
              is_team_member: false,
              content_preview: msg.content?.substring(0, 100) || '(no text)',
              created_at: msg.createdAt.toISOString(),
              is_answered: true,
              replied_at: new Date().toISOString(),
              replied_by: 'auto-reply-catchup',
              reply_time_seconds: Math.floor((Date.now() - msg.createdTimestamp) / 1000),
            }, { onConflict: 'discord_message_id,discord_server_id' });

            console.log(`[Catchup] Replied in #${channel.name} (${guild.name}): ${cleanReply.substring(0, 80)}...`);

            // Notify Slack
            await slackAlert(`Catchup auto-reply in ${guild.name} Discord`, [
              { type: 'section', text: { type: 'mrkdwn',
                text: `*#${channel.name}* — ${msg.author.username} asked:\n>${msg.content.substring(0, 200)}\n\n*Bot replied:*\n${cleanReply}` } },
              { type: 'context', elements: [{ type: 'mrkdwn', text: ':arrows_counterclockwise: Startup catchup — message was unanswered' }] },
            ]);

            // Throttle to avoid flooding
            await new Promise(r => setTimeout(r, CATCHUP_DELAY_MS));
          } catch (msgErr) {
            console.error(`[Catchup] Error on msg from ${msg.author.username}: ${msgErr.message}`);
            // Continue to next message — don't let one failure kill the loop
          }
        }
      } catch (err) {
        console.error(`[Catchup] Error in #${channel.name} (${guild.name}):`, err.message);
      }
    }
  }

  isCatchingUp = false;
  if (totalCaughtUp > 0) {
    console.log(`[Catchup] Replied to ${totalCaughtUp} unanswered message(s) across all servers`);
  } else {
    console.log(`[Catchup] No unanswered messages to catch up on`);
  }
}

// ── Event Handlers ──────────────────────────────────────────────────────────
client.on('ready', async () => {
  const serverNames = client.guilds.cache.map(g => `${g.name} (${g.memberCount} members)`);
  console.log(`Bot logged in as ${client.user.tag}`);
  console.log(`Auto-monitoring ${serverNames.length} server(s): ${serverNames.join(', ')}`);

  // Register /inspo slash command (global — available in every server)
  try {
    await client.application.commands.set([
      { name: 'inspo', description: 'Get top-performing creator content in your brand community' },
    ]);
    console.log('Registered /inspo slash command');
  } catch (err) {
    console.error('Slash command registration error:', err.message);
  }

  // Load bot config from Hub Supabase (non-blocking on failure — falls back to static JSON)
  await loadBotConfigFromDB().catch(err => console.error('Initial config load failed:', err.message));
  setInterval(loadBotConfigFromDB, CONFIG_POLL_MS);

  // Catch up on unanswered messages from before the bot was running
  await catchUpUnanswered().catch(err => console.error('Catchup error:', err.message));

  // Check unanswered every 5 minutes
  setInterval(checkUnanswered, 5 * 60 * 1000);

  // Daily snapshot at 23:55, digest at 09:00
  scheduleDaily(23, 55, takeMetricsSnapshot);
  scheduleDaily(9, 0, postDailyDigest);

  // Content inspiration — Tuesday & Thursday at 10:00
  scheduleWeekly(2, 10, 0, postWeeklyInspiration);
  scheduleWeekly(4, 10, 0, postWeeklyInspiration);

  // Engagement posts — Mon/Tue/Wed/Thu/Fri conversation starters
  scheduleEngagement();

  // Daily member checks at 10:00 — onboarding drip, content nudges, auto-roles, milestones
  scheduleDaily(10, 0, dailyMemberChecks);

  // Weekly creator spotlight — Wednesday at 11:00
  scheduleWeekly(3, 11, 0, postCreatorSpotlight);
});

// Auto-onboard new servers
client.on('guildCreate', async (guild) => {
  console.log(`Joined new server: ${guild.name} (${guild.id}, ${guild.memberCount} members)`);
  await slackAlert(`Bot joined new Discord server`, [
    { type: 'header', text: { type: 'plain_text', text: `New Server: ${guild.name}` } },
    { type: 'section', text: { type: 'mrkdwn',
      text: `*Server:* ${guild.name}\n*Members:* ${guild.memberCount}\n*ID:* ${guild.id}\n\nNow monitoring for unanswered messages and collecting community metrics.` } },
    { type: 'context', elements: [{ type: 'mrkdwn', text: ':white_check_mark: Auto-onboarded — no config needed' }] },
  ]);
});

client.on('guildDelete', async (guild) => {
  console.log(`Removed from server: ${guild.name} (${guild.id})`);
  pendingMessages.delete(guild.id);
  dailyActiveUsers.delete(guild.id);
});

client.on('messageCreate', async (message) => {
  if (!message.guild || message.author.bot) return;

  const isTeam = isTeamMember(message.member);

  // ── Spam / bad actor detection ──
  if (!isTeam) {
    const flagged = detectSpam(message);
    if (flagged) {
      try {
        await message.delete();
        console.log(`[Spam] Deleted message from ${message.author.username} in #${message.channel.name}: ${flagged.reason}`);
        // Alert team via Slack
        await slackAlert(`:rotating_light: Spam detected in ${message.guild.name}`, [
          { type: 'section', text: { type: 'mrkdwn',
            text: `*#${message.channel.name}* — ${message.author.username}\n*Reason:* ${flagged.reason}\n*Content:* ${message.content?.substring(0, 300) || '(no text)'}` } },
          { type: 'context', elements: [{ type: 'mrkdwn', text: flagged.kicked ? ':boot: User was kicked' : ':wastebasket: Message deleted — user not kicked (single offence)' }] },
        ]);
        // Alert in Discord team channel
        const alertChannel = await getOrCreateAlertChannel(message.guild);
        if (alertChannel) {
          await alertChannel.send(`**Spam detected** — deleted message from ${message.author.username} in #${message.channel.name}\nReason: ${flagged.reason}\nContent: \`${message.content?.substring(0, 200) || '(no text)'}\``);
        }
        // If severe (multiple offences), kick the user
        if (flagged.kicked && message.member?.kickable) {
          await message.member.kick('Automated spam detection');
          console.log(`[Spam] Kicked ${message.author.username} from ${message.guild.name}`);
        }
      } catch (err) {
        console.error(`[Spam] Error handling spam from ${message.author.username}: ${err.message}`);
      }
      return; // don't process further
    }
  }

  if (await handleProxyCommand(message)) return;
  if (await handleStatusCommand(message)) return;
  if (await handleInspireCommand(message)) return;

  await trackMessage(message, isTeam);

  if (isTeam) {
    // Team replied — cancel any pending auto-replies in this channel
    const serverPending = pendingMessages.get(message.guild.id);
    if (serverPending) {
      for (const [msgId, info] of serverPending.entries()) {
        if (info.channelId === message.channel.id) cancelAutoReply(msgId);
      }
    }
    if (message.reference?.messageId) {
      cancelAutoReply(message.reference.messageId);
      // FAQ learning — track team answers to community questions
      try {
        const originalMsg = await message.channel.messages.fetch(message.reference.messageId);
        if (originalMsg && !originalMsg.author.bot && originalMsg.content?.includes('?')) {
          trackTeamAnswer(message, originalMsg.content);
        }
      } catch (_) {} // original message might be deleted
    }
    await markAsAnswered(message);
  } else {
    // Community message — schedule auto-reply after delay
    scheduleAutoReply(message);

    // Win detection — celebrate achievements
    if (detectWin(message)) {
      await celebrateWin(message);
    }

    // Sentiment detection — fast-track alert for frustrated members
    const sentiment = detectSentiment(message);
    if (sentiment) {
      await handleNegativeSentiment(message, sentiment);
    }
  }
});

// ── /inspo — on-demand top performers, available to any community member ─────
const inspoCooldowns = new Map(); // userId → last-use timestamp

client.on('interactionCreate', async (interaction) => {
  if (!interaction.isChatInputCommand() || interaction.commandName !== 'inspo') return;

  if (!interaction.guild) {
    await interaction.reply({ content: 'Run this inside a brand community server.', ephemeral: true });
    return;
  }

  // Per-user cooldown (team members exempt) — content is cached anyway, this just stops spam
  const last = inspoCooldowns.get(interaction.user.id) || 0;
  if (Date.now() - last < 2 * 60 * 1000 && !isTeamMember(interaction.member)) {
    await interaction.reply({ content: 'Give it a couple of minutes before pulling `/inspo` again.', ephemeral: true });
    return;
  }
  inspoCooldowns.set(interaction.user.id, Date.now());

  await interaction.deferReply();
  try {
    const content = await buildInspoContent(interaction.guild);
    if (!content) {
      await interaction.editReply('No video performance data for this community yet — check back soon!');
      return;
    }
    await interaction.editReply(content.intro);
    for (const embed of content.embeds) {
      await interaction.followUp({ embeds: [embed] });
    }
  } catch (err) {
    console.error('/inspo error:', err.message);
    try { await interaction.editReply('Something went wrong pulling top performers — try again shortly.'); } catch (_) {}
  }
});

client.on('guildMemberAdd', async (member) => {
  console.log(`[${member.guild.name}] Joined: ${member.user.username}`);
  try {
    await supabase.from('discord_member_events').insert({
      discord_server_id: member.guild.id,
      user_id: member.user.id,
      username: member.user.username,
      event_type: 'join',
    });
  } catch (err) { console.error('Member join track error:', err.message); }

  // Welcome DM for conversational servers
  const brandCtx = getBrandContext(member.guild.id);
  if (brandCtx.conversational) {
    const displayName = member.displayName || member.user.displayName || member.user.username;
    const templates = [
      `Hey ${displayName}! Welcome to ${member.guild.name} :) Head over to #intro-yourself to say hi, and check out #brand-deals for any current opportunities. Let me know if you have any questions!`,
      `Welcome to ${member.guild.name}! I'm Alice from the team. Drop an intro in #intro-yourself when you get a chance, and keep an eye on #announcements for updates. Glad to have you here!`,
      `Hey! Welcome to ${member.guild.name} :) If you want to get started, check out #intro-yourself and #brand-deals. Let me know if you need anything!`,
    ];
    const dmText = templates[Math.floor(Math.random() * templates.length)];
    try {
      await member.send(dmText);
      console.log(`[Welcome DM] Sent to ${member.user.username} in ${member.guild.name}`);
      // Log to Slack so team can see every DM
      await slackAlert(`Alice DM'd new member in ${member.guild.name}`, [
        { type: 'section', text: { type: 'mrkdwn',
          text: `*New member:* ${member.user.username}\n*DM sent:*\n>${dmText}` } },
        { type: 'context', elements: [{ type: 'mrkdwn', text: ':wave: Welcome DM — team can see but member cannot tell' }] },
      ]);
    } catch (err) {
      console.log(`[Welcome DM] Could not DM ${member.user.username} (DMs may be disabled): ${err.message}`);
    }
  }
});

client.on('guildMemberRemove', async (member) => {
  console.log(`[${member.guild.name}] Left: ${member.user.username}`);
  try {
    await supabase.from('discord_member_events').insert({
      discord_server_id: member.guild.id,
      user_id: member.user.id,
      username: member.user.username,
      event_type: 'leave',
    });
  } catch (err) { console.error('Member leave track error:', err.message); }
});

// ── Scheduling ──────────────────────────────────────────────────────────────
function scheduleDaily(hour, minute, fn) {
  const now = new Date();
  let next = new Date(now);
  next.setHours(hour, minute, 0, 0);
  if (next <= now) next.setDate(next.getDate() + 1);
  const delay = next - now;
  setTimeout(() => { fn(); setInterval(fn, 86400000); }, delay);
  console.log(`Scheduled ${hour}:${String(minute).padStart(2, '0')} (next in ${Math.floor(delay / 3600000)}h ${Math.floor((delay % 3600000) / 60000)}m)`);
}

function scheduleWeekly(dayOfWeek, hour, minute, fn) {
  const days = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'];
  const now = new Date();
  let next = new Date(now);
  next.setHours(hour, minute, 0, 0);
  // Find next occurrence of the target day
  while (next.getDay() !== dayOfWeek || next <= now) {
    next.setDate(next.getDate() + 1);
  }
  const delay = next - now;
  setTimeout(() => { fn(); setInterval(fn, 7 * 86400000); }, delay);
  console.log(`Scheduled ${days[dayOfWeek]} ${hour}:${String(minute).padStart(2, '0')} weekly (next in ${Math.floor(delay / 86400000)}d ${Math.floor((delay % 86400000) / 3600000)}h)`);
}

// ── Start ───────────────────────────────────────────────────────────────────
console.log('Starting Discord Community Bot...');
client.login(process.env.DISCORD_BOT_TOKEN).catch(err => {
  console.error('Login failed:', err.message);
  process.exit(1);
});

process.on('SIGINT', () => { client.destroy(); process.exit(0); });
process.on('SIGTERM', () => { client.destroy(); process.exit(0); });
