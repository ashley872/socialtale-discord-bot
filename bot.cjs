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

// ── Brand Contexts (AI Auto-Reply) ──────────────────────────────────────────
const BRAND_CONTEXTS = JSON.parse(readFileSync(path.join(__dirname, 'brand-contexts.json'), 'utf8'));
const DEFAULT_BRAND = BRAND_CONTEXTS._default;
const AUTO_REPLY_DELAY_MS = (DEFAULT_BRAND.autoReplyDelayMinutes || 15) * 60 * 1000;
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

// ── Daily Metrics Snapshot (all servers) ─────────────────────────────────────
async function takeMetricsSnapshot() {
  const today = new Date().toISOString().split('T')[0];
  const todayStart = new Date(today + 'T00:00:00Z').toISOString();
  const todayEnd = new Date(today + 'T23:59:59Z').toISOString();

  for (const [, guild] of client.guilds.cache) {
    const serverId = guild.id;

    const { data: todayMessages } = await supabase.from('discord_messages')
      .select('author_id, is_team_member, is_answered')
      .eq('discord_server_id', serverId)
      .gte('created_at', todayStart).lte('created_at', todayEnd);

    const totalMsgs = todayMessages?.length || 0;
    const uniqueAuthors = new Set(todayMessages?.map(m => m.author_id) || []);
    const memberMessages = todayMessages?.filter(m => !m.is_team_member) || [];

    const { data: responseTimes } = await supabase.from('discord_messages')
      .select('reply_time_seconds')
      .eq('discord_server_id', serverId).eq('is_answered', true)
      .not('reply_time_seconds', 'is', null)
      .gte('created_at', todayStart).lte('created_at', todayEnd);

    const avgResponseTime = responseTimes?.length
      ? Math.round(responseTimes.reduce((a, b) => a + b.reply_time_seconds, 0) / responseTimes.length) : null;

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

    await supabase.from('discord_community_metrics').upsert({
      discord_server_id: serverId,
      server_name: guild.name,
      snapshot_date: today,
      total_members: guild.memberCount,
      online_members: guild.approximatePresenceCount || 0,
      total_messages: totalMsgs,
      unique_messagers: uniqueAuthors.size,
      messages_from_members: memberMessages.length,
      messages_answered: memberMessages.filter(m => m.is_answered).length,
      messages_unanswered: memberMessages.filter(m => !m.is_answered).length,
      avg_response_time_seconds: avgResponseTime,
      dau, wau, mau,
      stickiness_ratio: stickiness,
    }, { onConflict: 'discord_server_id,snapshot_date' });

    console.log(`Snapshot: ${guild.name} — ${totalMsgs} msgs, ${dau} DAU, ${mau} MAU`);
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

    // Discord embed
    const alertChannel = await getOrCreateAlertChannel(guild);
    if (alertChannel) {
      const embed = new EmbedBuilder()
        .setTitle(`Daily Community Digest — ${yesterday}`)
        .setColor(responseRate >= 90 ? 0x00CC66 : responseRate >= 70 ? 0xFFAA00 : 0xFF4444)
        .addFields(
          { name: 'Members', value: `${metrics.total_members} total`, inline: true },
          { name: 'Messages', value: `${metrics.total_messages} (${metrics.unique_messagers} people)`, inline: true },
          { name: 'Response Rate', value: `${responseRate}%`, inline: true },
          { name: 'Avg Response Time', value: `${avgMins} min`, inline: true },
          { name: 'Unanswered', value: `${metrics.messages_unanswered}`, inline: true },
          { name: 'DAU / WAU / MAU', value: `${metrics.dau} / ${metrics.wau} / ${metrics.mau}`, inline: true },
          { name: 'Stickiness', value: `${metrics.stickiness_ratio}%`, inline: true },
        ).setTimestamp();

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
        { type: 'mrkdwn', text: `*Messages:* ${metrics.total_messages} (${metrics.unique_messagers} people)` },
        { type: 'mrkdwn', text: `*Response Rate:* ${emoji} ${responseRate}%` },
        { type: 'mrkdwn', text: `*Avg Response:* ${avgMins} min` },
        { type: 'mrkdwn', text: `*Unanswered:* ${metrics.messages_unanswered}` },
        { type: 'mrkdwn', text: `*DAU / WAU / MAU:* ${metrics.dau} / ${metrics.wau} / ${metrics.mau}` },
        { type: 'mrkdwn', text: `*Stickiness:* ${metrics.stickiness_ratio}%` },
      ]},
    ];
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
  return { ...DEFAULT_BRAND, ...(BRAND_CONTEXTS[serverId] || {}) };
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

  const systemPrompt = `You are replying as ${brandCtx.brandName || 'the brand'} in a Discord community server for creators and affiliates.

Brand voice: ${brandCtx.brandVoice || DEFAULT_BRAND.brandVoice}

Brand context:
${contextInfo}

Known FAQ:
${faqEntries}

Rules:
- Reply in 1-3 sentences max. Be helpful and concise.
- Never say you're an AI, bot, or assistant. You ARE the brand team.
- If the message is just chit-chat, a greeting, or doesn't need a response, reply with exactly: NO_REPLY
- If you're not confident you can give an accurate, helpful answer, reply with exactly: NO_REPLY
- Do not make up information. Only use what's in the brand context and FAQ above.
- Match the casual, friendly tone of Discord. Use emojis naturally but sparingly.
- If someone asks something not covered in FAQ/context, reply with: NO_REPLY`;

  try {
    const response = await anthropic.messages.create({
      model: 'claude-haiku-4-5-20251001',
      max_tokens: 200,
      system: systemPrompt,
      messages: [{ role: 'user', content: `Discord message from ${message.author.displayName || message.author.username} in #${message.channel.name}:\n\n${message.content}` }],
    });

    const reply = response.content[0]?.text?.trim();
    if (!reply || reply === 'NO_REPLY') return null;
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
  const excludeChannels = brandCtx.excludeChannels || DEFAULT_BRAND.excludeChannels || [];
  const channelName = message.channel.name.replace(/[^\w-]/g, ''); // strip emojis for matching
  if (excludeChannels.some(ex => message.channel.name.includes(ex) || channelName.includes(ex))) return;

  const delayMs = (brandCtx.autoReplyDelayMinutes || DEFAULT_BRAND.autoReplyDelayMinutes || 15) * 60 * 1000;

  const timeoutId = setTimeout(async () => {
    pendingAutoReplies.delete(message.id);

    // Check if already answered by team
    const serverPending = pendingMessages.get(message.guild.id);
    if (!serverPending?.has(message.id)) return; // already answered

    // Rate limit check
    if (isAutoReplyRateLimited(message.guild.id)) return;

    // Generate reply
    const reply = await generateAutoReply(message, brandCtx);
    if (!reply) return;

    // Post via webhook (as brand) not as bot
    const webhook = await getOrCreateWebhook(message.channel, brandCtx.brandName || message.guild.name);
    if (webhook) {
      await webhook.send({
        content: reply,
        username: brandCtx.brandName || message.guild.name,
        avatarURL: message.guild.iconURL({ size: 128 }),
      });
    } else {
      // Fallback: reply as bot
      await message.reply(reply);
    }

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

    console.log(`Auto-replied in #${message.channel.name} (${message.guild.name}): ${reply.substring(0, 80)}...`);

    // Notify Slack that bot auto-replied
    await slackAlert(`Auto-replied in ${message.guild.name} Discord`, [
      { type: 'section', text: { type: 'mrkdwn',
        text: `*#${message.channel.name}* — ${message.author.username} asked:\n>${message.content.substring(0, 200)}\n\n*Bot replied:*\n${reply}` } },
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

// ── Event Handlers ──────────────────────────────────────────────────────────
client.on('ready', async () => {
  const serverNames = client.guilds.cache.map(g => `${g.name} (${g.memberCount} members)`);
  console.log(`Bot logged in as ${client.user.tag}`);
  console.log(`Auto-monitoring ${serverNames.length} server(s): ${serverNames.join(', ')}`);

  // Check unanswered every 5 minutes
  setInterval(checkUnanswered, 5 * 60 * 1000);

  // Daily snapshot at 23:55, digest at 09:00
  scheduleDaily(23, 55, takeMetricsSnapshot);
  scheduleDaily(9, 0, postDailyDigest);
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

  if (await handleProxyCommand(message)) return;
  if (await handleStatusCommand(message)) return;

  await trackMessage(message, isTeam);

  if (isTeam) {
    // Team replied — cancel any pending auto-replies in this channel
    const serverPending = pendingMessages.get(message.guild.id);
    if (serverPending) {
      for (const [msgId, info] of serverPending.entries()) {
        if (info.channelId === message.channel.id) cancelAutoReply(msgId);
      }
    }
    if (message.reference?.messageId) cancelAutoReply(message.reference.messageId);
    await markAsAnswered(message);
  } else {
    // Community message — schedule auto-reply after delay
    scheduleAutoReply(message);
  }
});

client.on('guildMemberAdd', (member) => {
  console.log(`[${member.guild.name}] Joined: ${member.user.username}`);
});

client.on('guildMemberRemove', (member) => {
  console.log(`[${member.guild.name}] Left: ${member.user.username}`);
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

// ── Start ───────────────────────────────────────────────────────────────────
console.log('Starting Discord Community Bot...');
client.login(process.env.DISCORD_BOT_TOKEN).catch(err => {
  console.error('Login failed:', err.message);
  process.exit(1);
});

process.on('SIGINT', () => { client.destroy(); process.exit(0); });
process.on('SIGTERM', () => { client.destroy(); process.exit(0); });
