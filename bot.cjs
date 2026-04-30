/**
 * Discord Community Bot — Social Tale
 *
 * Features:
 * 1. Unanswered message tracking — alerts when community messages go unanswered
 * 2. Proxy posting — team posts as brand via /say command
 * 3. Community metrics — daily snapshots of engagement, retention, growth
 * 4. Daily digest — summary of community health posted to team channel
 *
 * Run: node scripts/discord-community-bot.cjs
 * Keeps running as a long-lived process (use pm2/launchd to manage)
 */

const { Client, GatewayIntentBits, PermissionFlagsBits, EmbedBuilder, WebhookClient } = require('discord.js');
const { WebClient } = require('@slack/web-api');
const { createClient } = require('@supabase/supabase-js');
const { readFileSync } = require('fs');
const path = require('path');

// On Railway, env vars are set directly. Locally, load from .env.local
require('dotenv').config({ path: path.join(__dirname, '.env.local') });
require('dotenv').config({ path: path.join(__dirname, '../.env.local') });

// ── Config ──────────────────────────────────────────────────────────────────
const CONFIG = JSON.parse(readFileSync(path.join(__dirname, 'discord-servers.json'), 'utf8'));
const SERVERS = CONFIG.servers.filter(s => s.enabled);
const DEFAULTS = CONFIG.defaults;

const supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_KEY);

// ── Slack ───────────────────────────────────────────────────────────────────
const slack = process.env.SLACK_BOT_TOKEN ? new WebClient(process.env.SLACK_BOT_TOKEN) : null;
const SLACK_COMMUNITY_CHANNEL = CONFIG.slackChannelId || null;

async function slackAlert(text, blocks) {
  if (!slack || !SLACK_COMMUNITY_CHANNEL) return;
  try {
    await slack.chat.postMessage({
      channel: SLACK_COMMUNITY_CHANNEL,
      text,
      blocks,
      unfurl_links: false,
    });
  } catch (err) {
    console.error('Slack alert error:', err.message);
  }
}

// In-memory tracking for unanswered messages
// Map<serverId, Map<messageId, { channelId, authorName, contentPreview, timestamp }>>
const pendingMessages = new Map();

// Track active members per server for DAU/WAU/MAU
// Map<serverId, Set<authorId>>
const dailyActiveUsers = new Map();

// Webhook cache: Map<channelId, Webhook>
const webhookCache = new Map();

// ── Discord Client Setup ────────────────────────────────────────────────────
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
function getServerConfig(guildId) {
  return SERVERS.find(s => s.id === guildId);
}

function isTeamMember(member, serverConfig) {
  if (!member) return false;

  // Check by role name
  if (serverConfig.teamRole && member.roles) {
    const roleName = serverConfig.teamRole.toLowerCase();
    if (member.roles.cache.some(r => r.name.toLowerCase() === roleName)) return true;
  }

  // Check by username list
  if (serverConfig.teamUsernames?.length) {
    const username = member.user?.username?.toLowerCase();
    if (username && serverConfig.teamUsernames.some(u => u.toLowerCase() === username)) return true;
  }

  return false;
}

async function getOrCreateAlertChannel(guild, channelName) {
  let channel = guild.channels.cache.find(
    c => c.name === channelName && c.type === 0 // GuildText
  );
  if (!channel) {
    try {
      channel = await guild.channels.create({
        name: channelName,
        type: 0,
        permissionOverwrites: [
          {
            id: guild.id, // @everyone — hide from community
            deny: [PermissionFlagsBits.ViewChannel],
          },
          {
            id: client.user.id, // bot can see + send
            allow: [PermissionFlagsBits.ViewChannel, PermissionFlagsBits.SendMessages],
          },
        ],
      });
      // Note: team members will need to be manually granted access to this channel
      // since there's no team role. Consider creating a "Team" role for easier management.
      console.log(`Created #${channelName} in ${guild.name}`);
    } catch (err) {
      console.error(`Could not create #${channelName} in ${guild.name}:`, err.message);
      return null;
    }
  }
  return channel;
}

async function getOrCreateWebhook(channel, brandName, brandAvatar) {
  const key = channel.id;
  if (webhookCache.has(key)) return webhookCache.get(key);

  try {
    const webhooks = await channel.fetchWebhooks();
    let webhook = webhooks.find(w => w.owner?.id === client.user.id);
    if (!webhook) {
      webhook = await channel.createWebhook({
        name: brandName || 'Brand',
        reason: 'Social Tale proxy posting',
      });
    }
    webhookCache.set(key, webhook);
    return webhook;
  } catch (err) {
    console.error(`Could not get/create webhook in #${channel.name}:`, err.message);
    return null;
  }
}

// ── Message Tracking ────────────────────────────────────────────────────────
async function trackMessage(message, serverConfig, isTeam) {
  const preview = message.content?.substring(0, 100) || '(no text)';

  // Store in DB
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
      is_answered: isTeam, // team messages are inherently "answered"
    }, { onConflict: 'discord_message_id,discord_server_id' });
  } catch (err) {
    console.error('DB track error:', err.message);
  }

  // Track DAU
  if (!dailyActiveUsers.has(message.guild.id)) {
    dailyActiveUsers.set(message.guild.id, new Set());
  }
  dailyActiveUsers.get(message.guild.id).add(message.author.id);

  // If member message, add to pending for unanswered check
  if (!isTeam) {
    if (!pendingMessages.has(message.guild.id)) {
      pendingMessages.set(message.guild.id, new Map());
    }
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

async function markAsAnswered(message, serverConfig) {
  // Check if this is a reply to a pending message in the same channel
  const serverPending = pendingMessages.get(message.guild.id);
  if (!serverPending) return;

  // If it's a direct reply, mark that specific message
  if (message.reference?.messageId && serverPending.has(message.reference.messageId)) {
    serverPending.delete(message.reference.messageId);
    const replyTime = Math.floor((Date.now() - Date.now()) / 1000); // approx
    await supabase.from('discord_messages')
      .update({
        is_answered: true,
        replied_at: new Date().toISOString(),
        replied_by: message.author.displayName || message.author.username,
      })
      .eq('discord_message_id', message.reference.messageId)
      .eq('discord_server_id', message.guild.id);
    return;
  }

  // Otherwise, mark all pending messages in this channel as answered
  // (team member posting in a channel = engaging with the conversation)
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

// ── Unanswered Alert Check ──────────────────────────────────────────────────
async function checkUnanswered() {
  for (const serverConfig of SERVERS) {
    const serverPending = pendingMessages.get(serverConfig.id);
    if (!serverPending || serverPending.size === 0) continue;

    const thresholdMs = (serverConfig.responseTimeMinutes || DEFAULTS.responseTimeMinutes) * 60 * 1000;
    const now = Date.now();
    const overdue = [];

    for (const [msgId, info] of serverPending.entries()) {
      if (now - info.timestamp > thresholdMs) {
        overdue.push({ msgId, ...info });
      }
    }

    if (overdue.length === 0) continue;

    // Send alert
    const guild = client.guilds.cache.get(serverConfig.id);
    if (!guild) continue;

    const alertChannel = await getOrCreateAlertChannel(
      guild,
      serverConfig.alertChannelName || DEFAULTS.alertChannelName
    );
    if (!alertChannel) continue;

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

      // Mark as alerted so we don't spam
      serverPending.delete(msg.msgId);
      await supabase.from('discord_messages')
        .update({ alerted: true })
        .eq('discord_message_id', msg.msgId)
        .eq('discord_server_id', serverConfig.id);
    }

    if (overdue.length > 10) {
      embed.setFooter({ text: `...and ${overdue.length - 10} more` });
    }

    await alertChannel.send({ embeds: [embed] });
    console.log(`Alerted ${overdue.length} unanswered in ${guild.name}`);

    // Also alert in Slack
    const slackBlocks = [
      {
        type: 'header',
        text: { type: 'plain_text', text: `${overdue.length} Unanswered in ${serverConfig.name} Discord` },
      },
      ...overdue.slice(0, 10).map(msg => ({
        type: 'section',
        text: {
          type: 'mrkdwn',
          text: `*#${msg.channelName}* — ${msg.authorName} (${Math.floor((now - msg.timestamp) / 60000)}m ago)\n>${msg.contentPreview}`,
        },
        accessory: {
          type: 'button',
          text: { type: 'plain_text', text: 'Open in Discord' },
          url: msg.messageUrl,
        },
      })),
    ];
    await slackAlert(`${overdue.length} unanswered message(s) in ${serverConfig.name} Discord`, slackBlocks);
  }
}

// ── Proxy Posting ───────────────────────────────────────────────────────────
async function handleProxyCommand(message, serverConfig) {
  // Format: !say #channel-name Message text here
  // or:     !say Message text here (posts in same channel)
  if (!message.content.startsWith('!say ')) return false;

  const isTeam = isTeamMember(message.member, serverConfig);
  if (!isTeam) return false;

  const parts = message.content.slice(5).trim();
  let targetChannel = message.channel;
  let text = parts;

  // Check if first word is a channel mention
  const channelMatch = parts.match(/^<#(\d+)>\s+(.+)$/s);
  if (channelMatch) {
    targetChannel = message.guild.channels.cache.get(channelMatch[1]) || message.channel;
    text = channelMatch[2];
  }

  const webhook = await getOrCreateWebhook(targetChannel, serverConfig.name);
  if (!webhook) {
    await message.reply('Could not create webhook for proxy posting.');
    return true;
  }

  // Get guild icon as brand avatar
  const guildIcon = message.guild.iconURL({ size: 128 });

  await webhook.send({
    content: text,
    username: serverConfig.name,
    avatarURL: guildIcon,
  });

  // Delete the command message to keep it clean
  try { await message.delete(); } catch (_) {}
  console.log(`Proxy posted in #${targetChannel.name} for ${serverConfig.name}`);
  return true;
}

// ── Daily Metrics Snapshot ──────────────────────────────────────────────────
async function takeMetricsSnapshot() {
  const today = new Date().toISOString().split('T')[0];

  for (const serverConfig of SERVERS) {
    const guild = client.guilds.cache.get(serverConfig.id);
    if (!guild) continue;

    // Fetch member count
    let totalMembers = guild.memberCount;
    let onlineMembers = guild.approximatePresenceCount || 0;

    // Count today's messages from DB
    const todayStart = new Date(today + 'T00:00:00Z').toISOString();
    const todayEnd = new Date(today + 'T23:59:59Z').toISOString();

    const { data: todayMessages } = await supabase.from('discord_messages')
      .select('author_id, is_team_member, is_answered')
      .eq('discord_server_id', serverConfig.id)
      .gte('created_at', todayStart)
      .lte('created_at', todayEnd);

    const totalMsgs = todayMessages?.length || 0;
    const uniqueAuthors = new Set(todayMessages?.map(m => m.author_id) || []);
    const memberMessages = todayMessages?.filter(m => !m.is_team_member) || [];
    const answeredMsgs = memberMessages.filter(m => m.is_answered);
    const unansweredMsgs = memberMessages.filter(m => !m.is_answered);

    // Avg response time for answered messages today
    const { data: responseTimes } = await supabase.from('discord_messages')
      .select('reply_time_seconds')
      .eq('discord_server_id', serverConfig.id)
      .eq('is_answered', true)
      .not('reply_time_seconds', 'is', null)
      .gte('created_at', todayStart)
      .lte('created_at', todayEnd);

    const avgResponseTime = responseTimes?.length
      ? Math.round(responseTimes.reduce((a, b) => a + b.reply_time_seconds, 0) / responseTimes.length)
      : null;

    // WAU: unique authors in last 7 days
    const weekAgo = new Date(Date.now() - 7 * 86400000).toISOString();
    const { data: weekMessages } = await supabase.from('discord_messages')
      .select('author_id')
      .eq('discord_server_id', serverConfig.id)
      .gte('created_at', weekAgo);
    const wau = new Set(weekMessages?.map(m => m.author_id) || []).size;

    // MAU: unique authors in last 30 days
    const monthAgo = new Date(Date.now() - 30 * 86400000).toISOString();
    const { data: monthMessages } = await supabase.from('discord_messages')
      .select('author_id')
      .eq('discord_server_id', serverConfig.id)
      .gte('created_at', monthAgo);
    const mau = new Set(monthMessages?.map(m => m.author_id) || []).size;

    const dau = dailyActiveUsers.get(serverConfig.id)?.size || uniqueAuthors.size;
    const stickiness = mau > 0 ? Math.round((dau / mau) * 10000) / 100 : 0;

    await supabase.from('discord_community_metrics').upsert({
      discord_server_id: serverConfig.id,
      server_name: serverConfig.name,
      snapshot_date: today,
      total_members: totalMembers,
      online_members: onlineMembers,
      total_messages: totalMsgs,
      unique_messagers: uniqueAuthors.size,
      messages_from_members: memberMessages.length,
      messages_answered: answeredMsgs.length,
      messages_unanswered: unansweredMsgs.length,
      avg_response_time_seconds: avgResponseTime,
      dau,
      wau,
      mau,
      stickiness_ratio: stickiness,
    }, { onConflict: 'discord_server_id,snapshot_date' });

    console.log(`Snapshot saved for ${serverConfig.name} (${today}): ${totalMsgs} msgs, ${dau} DAU, ${mau} MAU`);

    // Reset daily active users
    dailyActiveUsers.set(serverConfig.id, new Set());
  }
}

// ── Daily Digest ────────────────────────────────────────────────────────────
async function postDailyDigest() {
  const yesterday = new Date(Date.now() - 86400000).toISOString().split('T')[0];

  for (const serverConfig of SERVERS) {
    const guild = client.guilds.cache.get(serverConfig.id);
    if (!guild) continue;

    const { data: metrics } = await supabase.from('discord_community_metrics')
      .select('*')
      .eq('discord_server_id', serverConfig.id)
      .eq('snapshot_date', yesterday)
      .single();

    if (!metrics) continue;

    const alertChannel = await getOrCreateAlertChannel(
      guild,
      serverConfig.digestChannelName || DEFAULTS.digestChannelName
    );
    if (!alertChannel) continue;

    const responseRate = metrics.messages_from_members > 0
      ? Math.round((metrics.messages_answered / metrics.messages_from_members) * 100)
      : 100;

    const avgMins = metrics.avg_response_time_seconds
      ? Math.round(metrics.avg_response_time_seconds / 60)
      : 'N/A';

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
      )
      .setTimestamp();

    if (metrics.messages_unanswered > 0) {
      // List still-unanswered messages from yesterday
      const { data: unanswered } = await supabase.from('discord_messages')
        .select('channel_name, author_name, content_preview')
        .eq('discord_server_id', serverConfig.id)
        .eq('is_answered', false)
        .eq('is_team_member', false)
        .gte('created_at', yesterday + 'T00:00:00Z')
        .lte('created_at', yesterday + 'T23:59:59Z')
        .limit(5);

      if (unanswered?.length) {
        const list = unanswered
          .map(m => `• **#${m.channel_name}** — ${m.author_name}: ${m.content_preview}`)
          .join('\n');
        embed.addFields({ name: 'Still Unanswered', value: list });
      }
    }

    await alertChannel.send({ embeds: [embed] });
    console.log(`Digest posted for ${serverConfig.name}`);

    // Post digest to Slack too
    const emoji = responseRate >= 90 ? ':large_green_circle:' : responseRate >= 70 ? ':large_orange_circle:' : ':red_circle:';
    const slackDigestBlocks = [
      {
        type: 'header',
        text: { type: 'plain_text', text: `${serverConfig.name} Community Digest — ${yesterday}` },
      },
      {
        type: 'section',
        fields: [
          { type: 'mrkdwn', text: `*Members:* ${metrics.total_members}` },
          { type: 'mrkdwn', text: `*Messages:* ${metrics.total_messages} (${metrics.unique_messagers} people)` },
          { type: 'mrkdwn', text: `*Response Rate:* ${emoji} ${responseRate}%` },
          { type: 'mrkdwn', text: `*Avg Response:* ${avgMins} min` },
          { type: 'mrkdwn', text: `*Unanswered:* ${metrics.messages_unanswered}` },
          { type: 'mrkdwn', text: `*DAU / WAU / MAU:* ${metrics.dau} / ${metrics.wau} / ${metrics.mau}` },
          { type: 'mrkdwn', text: `*Stickiness:* ${metrics.stickiness_ratio}%` },
        ],
      },
    ];

    if (metrics.messages_unanswered > 0) {
      slackDigestBlocks.push({
        type: 'section',
        text: {
          type: 'mrkdwn',
          text: `:warning: *${metrics.messages_unanswered} message(s) still unanswered from yesterday*`,
        },
      });
    }

    await slackAlert(`${serverConfig.name} Community Digest — ${yesterday}`, slackDigestBlocks);
  }
}

// ── Status Command ──────────────────────────────────────────────────────────
async function handleStatusCommand(message, serverConfig) {
  if (message.content !== '!community-status') return false;
  if (!isTeamMember(message.member, serverConfig)) return false;

  const today = new Date().toISOString().split('T')[0];
  const serverPending = pendingMessages.get(serverConfig.id);
  const pendingCount = serverPending?.size || 0;

  // Get recent metrics
  const { data: recent } = await supabase.from('discord_community_metrics')
    .select('*')
    .eq('discord_server_id', serverConfig.id)
    .order('snapshot_date', { ascending: false })
    .limit(7);

  const embed = new EmbedBuilder()
    .setTitle(`Community Status — ${serverConfig.name}`)
    .setColor(0x5865F2)
    .addFields(
      { name: 'Pending (unanswered)', value: `${pendingCount} messages`, inline: true },
    )
    .setTimestamp();

  if (recent?.length) {
    const latest = recent[0];
    embed.addFields(
      { name: 'Members', value: `${latest.total_members}`, inline: true },
      { name: 'MAU', value: `${latest.mau}`, inline: true },
      { name: 'Stickiness', value: `${latest.stickiness_ratio}%`, inline: true },
    );

    // 7-day trend
    if (recent.length >= 2) {
      const msgTrend = recent.reduce((a, b) => a + (b.total_messages || 0), 0);
      embed.addFields({ name: '7-Day Messages', value: `${msgTrend}`, inline: true });
    }
  }

  await message.reply({ embeds: [embed] });
  return true;
}

// ── Event Handlers ──────────────────────────────────────────────────────────
client.on('ready', () => {
  console.log(`Discord bot logged in as ${client.user.tag}`);
  console.log(`Monitoring ${SERVERS.length} server(s): ${SERVERS.map(s => s.name).join(', ')}`);

  // Check for unanswered messages every 5 minutes
  setInterval(checkUnanswered, 5 * 60 * 1000);

  // Schedule daily snapshot at 23:55
  scheduleDaily(23, 55, async () => {
    await takeMetricsSnapshot();
  });

  // Schedule daily digest at 09:00
  scheduleDaily(9, 0, async () => {
    await postDailyDigest();
  });
});

client.on('messageCreate', async (message) => {
  // Ignore DMs and bot messages
  if (!message.guild) return;
  if (message.author.bot) return;

  const serverConfig = getServerConfig(message.guild.id);
  if (!serverConfig) return;

  // Check excluded channels
  if (serverConfig.excludeChannels?.includes(message.channel.name)) return;

  const isTeam = isTeamMember(message.member, serverConfig);

  // Handle commands first
  if (await handleProxyCommand(message, serverConfig)) return;
  if (await handleStatusCommand(message, serverConfig)) return;

  // Track the message
  await trackMessage(message, serverConfig, isTeam);

  // If team member, mark pending messages in this channel as answered
  if (isTeam) {
    await markAsAnswered(message, serverConfig);
  }
});

// Track member joins/leaves for growth metrics
client.on('guildMemberAdd', async (member) => {
  const serverConfig = getServerConfig(member.guild.id);
  if (!serverConfig) return;
  console.log(`[${serverConfig.name}] Member joined: ${member.user.username}`);
});

client.on('guildMemberRemove', async (member) => {
  const serverConfig = getServerConfig(member.guild.id);
  if (!serverConfig) return;
  console.log(`[${serverConfig.name}] Member left: ${member.user.username}`);
});

// ── Scheduling Helper ───────────────────────────────────────────────────────
function scheduleDaily(hour, minute, fn) {
  const now = new Date();
  let next = new Date(now);
  next.setHours(hour, minute, 0, 0);
  if (next <= now) next.setDate(next.getDate() + 1);

  const delay = next - now;
  setTimeout(() => {
    fn();
    // Then repeat every 24h
    setInterval(fn, 24 * 60 * 60 * 1000);
  }, delay);

  const hrs = Math.floor(delay / 3600000);
  const mins = Math.floor((delay % 3600000) / 60000);
  console.log(`Scheduled daily task at ${hour}:${String(minute).padStart(2, '0')} (next in ${hrs}h ${mins}m)`);
}

// ── Start ───────────────────────────────────────────────────────────────────
console.log('Starting Discord Community Bot...');
client.login(process.env.DISCORD_BOT_TOKEN).catch(err => {
  console.error('Login failed:', err.message);
  process.exit(1);
});

// Graceful shutdown
process.on('SIGINT', () => {
  console.log('Shutting down...');
  client.destroy();
  process.exit(0);
});

process.on('SIGTERM', () => {
  console.log('Shutting down...');
  client.destroy();
  process.exit(0);
});
