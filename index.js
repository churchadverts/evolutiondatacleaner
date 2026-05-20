import { WebSocket } from 'ws';
globalThis.WebSocket = WebSocket;

import express from 'express';
import { createClient } from '@supabase/supabase-js';
import dotenv from 'dotenv';

dotenv.config();

const app = express();
app.use(express.json({ limit: '50mb' }));

const supabase = createClient(
    process.env.SUPABASE_URL,
    process.env.SUPABASE_SERVICE_KEY,
    { auth: { persistSession: false } }
);

const HISTORY_DAYS         = parseInt(process.env.HISTORY_DAYS || '90');
const LEAPCELL_PERSONA_URL = process.env.LEAPCELL_PERSONA_URL || null;

// ==========================================
// 1. HELPERS
// ==========================================

function isGroupOrBroadcast(jid) {
    if (!jid) return true;
    return (
        jid.endsWith('@g.us')      ||
        jid.endsWith('@broadcast') ||
        jid === 'status@broadcast' ||
        jid.includes('newsletter') ||
        jid.includes('lid')
    );
}

function extractPhone(jid) {
    if (!jid) return null;
    const raw = jid.split('@')[0].replace(/\D/g, '');
    if (!raw || raw.length < 7) return null;
    return `+${raw}`;
}

function extractMessageContent(msg) {
    const m = msg.message;
    if (!m) return null;

    if (m.conversation)
        return { text: m.conversation, type: 'text' };

    if (m.extendedTextMessage?.text)
        return { text: m.extendedTextMessage.text, type: 'text' };

    if (m.imageMessage)
        return { text: m.imageMessage.caption || '', type: 'image', caption: m.imageMessage.caption || '' };

    if (m.audioMessage)
        return { text: '', type: m.audioMessage.ptt ? 'voice_note' : 'audio', duration_seconds: m.audioMessage.seconds || null };

    if (m.videoMessage)
        return { text: m.videoMessage.caption || '', type: 'video', caption: m.videoMessage.caption || '' };

    if (m.documentMessage)
        return { text: m.documentMessage.fileName || '', type: 'document', file_name: m.documentMessage.fileName || '' };

    if (m.stickerMessage)
        return { text: '', type: 'sticker' };

    if (m.reactionMessage)
        return { text: m.reactionMessage.text || '', type: 'reaction', emoji: m.reactionMessage.text || '' };

    if (m.locationMessage)
        return {
            text:      `Location: ${m.locationMessage.degreesLatitude}, ${m.locationMessage.degreesLongitude}`,
            type:      'location',
            latitude:  m.locationMessage.degreesLatitude,
            longitude: m.locationMessage.degreesLongitude
        };

    if (m.contactMessage)
        return { text: m.contactMessage.displayName || 'Contact shared', type: 'contact' };

    if (m.buttonsResponseMessage)
        return { text: m.buttonsResponseMessage.selectedDisplayText || '', type: 'button_response' };

    if (m.listResponseMessage)
        return { text: m.listResponseMessage.title || '', type: 'list_response' };

    return null;
}

// ==========================================
// 2. SYSTEM 1 TRIGGER
// ==========================================

async function triggerPersonaPackGeneration(businessId) {
    if (!LEAPCELL_PERSONA_URL) {
        console.log(`  [System1] LEAPCELL_PERSONA_URL not set — skipping for ${businessId}`);
        return;
    }
    try {
        const res = await fetch(LEAPCELL_PERSONA_URL, {
            method:  'POST',
            headers: { 'Content-Type': 'application/json' },
            body:    JSON.stringify({ business_id: businessId })
        });
        if (res.ok) {
            console.log(`  [System1] ✓ Persona pack generation triggered for ${businessId}`);
        } else {
            console.error(`  [System1] ✗ HTTP ${res.status}`);
        }
    } catch (e) {
        console.error(`  [System1] ✗ ${e.message}`);
    }
}

// ==========================================
// 3. CONTACT + CONVERSATION HELPERS
// ==========================================

/**
 * Finds an existing contact by WhatsApp JID or creates a new one.
 * Only updates name if we have a pushName and the current name is blank/Unknown.
 * Returns { id, lead_state, name }
 */
async function getOrCreateContact(businessId, jid, pushName) {
    const phone = extractPhone(jid);

    // Check if contact exists
    const { data: existing } = await supabase
        .from('contacts')
        .select('id, lead_state, name')
        .eq('sasa_business_id', businessId)
        .eq('social_platform', 'whatsapp')
        .eq('social_id', jid)
        .maybeSingle();

    if (existing) {
        // Update last_seen; update name only if we have a better one
        const updates = { last_seen: new Date().toISOString() };
        if (pushName && (!existing.name || existing.name === 'Unknown')) {
            updates.name = pushName;
        }
        await supabase.from('contacts').update(updates).eq('id', existing.id);
        return existing;
    }

    // Create new contact
    const { data: created, error } = await supabase
        .from('contacts')
        .insert({
            sasa_business_id: businessId,
            social_platform: 'whatsapp',
            social_id:       jid,
            name:            pushName || 'Unknown',
            phone:           phone,
            lead_state:      'new',
            follow_up_count: 0,
            last_seen:       new Date().toISOString()
        })
        .select('id, lead_state, name')
        .single();

    if (error) throw new Error(`Contact create failed: ${error.message}`);
    console.log(`  [Contact] New contact created: ${created.id} (${pushName || jid})`);
    return created;
}

/**
 * Finds an existing conversation for this business+contact or creates one.
 * Returns the conversation id.
 */
async function getOrCreateConversation(businessId, contactId, jid) {
    const { data: existing } = await supabase
        .from('conversations')
        .select('id')
        .eq('sasa_business_id', businessId)
        .eq('contact_id', contactId)
        .maybeSingle();

    if (existing) return existing.id;

    const { data: created, error } = await supabase
        .from('conversations')
        .insert({
            sasa_business_id: businessId,
            contact_id:   contactId,
            external_id:  jid,
            channel:      'whatsapp',
            type:         'dm',
            status:       'open',
            unread_count: 0,
            ai_enabled:   true,
            active_agent: 'manager'
        })
        .select('id')
        .single();

    if (error) throw new Error(`Conversation create failed: ${error.message}`);
    console.log(`  [Conversation] New conversation created: ${created.id}`);
    return created.id;
}

/**
 * Lead state machine — transitions lead to 'engaged' when they send a message.
 * Won, lost, and do_not_contact leads are left untouched.
 */
async function updateLeadStateOnReply(contactId, currentState) {
    const transitionStates = ['new', 'stalled', 'ghosted', 'warm'];
    if (!transitionStates.includes(currentState)) return;

    const { error } = await supabase
        .from('contacts')
        .update({ lead_state: 'engaged' })
        .eq('id', contactId);

    if (error) {
        console.error(`  [State] Lead state update failed: ${error.message}`);
    } else {
        console.log(`  [State] ${contactId}: ${currentState} → engaged`);
    }
}

/**
 * Cancels all pending follow-ups for a lead when they reply.
 * A reply means the follow-up worked or is no longer needed.
 */
async function cancelPendingFollowUps(contactId) {
    const { data, error } = await supabase
        .from('follow_up_queue')
        .update({
            status:      'cancelled',
            skip_reason: 'lead_replied'
        })
        .eq('contact_id', contactId)
        .eq('status', 'pending')
        .select('id');

    if (error) {
        console.error(`  [Queue] Cancel follow-ups failed: ${error.message}`);
    } else if (data?.length > 0) {
        console.log(`  [Queue] ${data.length} pending follow-up(s) cancelled for contact ${contactId}`);
    }
}

/**
 * Processes consent responses (STOP / YES) from inbound messages.
 * Detects patterns in multiple languages and updates contact consent status.
 */
async function processConsentResponse(contactId, contentText) {
    const text = contentText?.trim().toLowerCase() || '';

    const isStop = /\bstop\b|hapana|usiteme|acha|unsubscribe|opt.?out/i.test(text);
    const isYes  = /\byes\b|\bndio\b|\bok\b|\bsawa\b|\bokay\b|subscribe|nipe|tuma/i.test(text);

    if (isStop) {
        await supabase.from('contacts').update({
            do_not_contact:        true,
            follow_up_opted_in:    false,
            follow_up_opted_out_at: new Date().toISOString()
        }).eq('id', contactId);

        // Kill every pending follow-up for this contact — permanent
        await supabase.from('follow_up_queue').update({
            status:      'cancelled',
            skip_reason: 'contact_opted_out'
        }).eq('contact_id', contactId).eq('status', 'pending');

        console.log(`  [Consent] STOP received — contact ${contactId} permanently removed`);
    }

    if (isYes) {
        const { data: contact } = await supabase
            .from('contacts')
            .select('consent_message_sent_at, follow_up_opted_in')
            .eq('id', contactId)
            .single();

        // Only process YES if we actually sent a consent message to them
        if (contact?.consent_message_sent_at && !contact?.follow_up_opted_in) {
            await supabase.from('contacts').update({
                follow_up_opted_in:    true,
                follow_up_opted_in_at: new Date().toISOString()
            }).eq('id', contactId);

            console.log(`  [Consent] YES received — contact ${contactId} opted in`);
            // Phase 4 will pick this up and schedule Step 1
        }
    }
}

// ==========================================
// 4. HISTORY SYNC (unchanged from Phase 2)
// ==========================================

async function processHistorySync(payload, businessId) {
    const contacts = payload.data?.contacts || [];
    const chats    = payload.data?.chats    || [];
    const messages = payload.data?.messages || [];

    const stats = { contacts: 0, conversations: 0, messages: 0, skipped: 0 };
    console.log(`  Raw — contacts:${contacts.length} chats:${chats.length} messages:${messages.length}`);

    // ── Contacts ─────────────────────────────────────────────────
    const validContacts    = contacts.filter(c => c.id && !isGroupOrBroadcast(c.id));
    const contactPayloads  = validContacts.map(c => ({
        sasa_business_id: businessId,
        social_platform: 'whatsapp',
        social_id:       c.id,
        name:            c.name || c.notify || c.verifiedName || 'Unknown',
        phone:           extractPhone(c.id),
        lead_state:      'new'
    }));

    let contactMap = {};
    if (contactPayloads.length > 0) {
        const { data: inserted, error } = await supabase
            .from('contacts')
            .upsert(contactPayloads, {
                onConflict:       'sasa_business_id, social_platform, social_id',
                ignoreDuplicates: false
            })
            .select('id, social_id');

        if (error) throw new Error(`Contacts upsert: ${error.message}`);
        contactMap     = inserted.reduce((acc, c) => { acc[c.social_id] = c.id; return acc; }, {});
        stats.contacts = inserted.length;
        console.log(`  [Contacts] ${stats.contacts} upserted`);
    }

    // ── Conversations ────────────────────────────────────────────
    const validChats          = chats.filter(c => c.id && !isGroupOrBroadcast(c.id));
    const conversationPayloads = validChats
        .map(chat => ({
            sasa_business_id: businessId,
            contact_id:   contactMap[chat.id],
            external_id:  chat.id,
            channel:      'whatsapp',
            type:         'dm',
            status:       (chat.unreadCount || 0) > 0 ? 'open' : 'closed',
            unread_count: chat.unreadCount || 0,
            ai_enabled:   true,
            active_agent: 'manager'
        }))
        .filter(c => c.contact_id != null);

    let convoMap = {};
    if (conversationPayloads.length > 0) {
        const { data: inserted, error } = await supabase
            .from('conversations')
            .upsert(conversationPayloads, { onConflict: 'sasa_business_id, contact_id' })
            .select('id, external_id');

        if (error) throw new Error(`Conversations upsert: ${error.message}`);
        convoMap           = inserted.reduce((acc, c) => { acc[c.external_id] = c.id; return acc; }, {});
        stats.conversations = inserted.length;
        console.log(`  [Conversations] ${stats.conversations} upserted`);
    }

    // ── Messages ─────────────────────────────────────────────────
    const cutoffTs        = Math.floor(Date.now() / 1000) - (HISTORY_DAYS * 24 * 60 * 60);
    const convLastMsg     = {};
    const convLastInbound = {};
    const messagePayloads = [];

    for (const msg of messages) {
        const jid = msg.key?.remoteJid;
        if (!jid || isGroupOrBroadcast(jid))        { stats.skipped++; continue; }
        if ((msg.messageTimestamp || 0) < cutoffTs) { stats.skipped++; continue; }
        if (msg.messageStubType)                     { stats.skipped++; continue; }

        const conversationId = convoMap[jid];
        const contactId      = contactMap[jid] || null;
        if (!conversationId)                         { stats.skipped++; continue; }

        const content = extractMessageContent(msg);
        if (!content)                                { stats.skipped++; continue; }

        const isFromMe = msg.key.fromMe === true;
        const ts       = msg.messageTimestamp;

        if (!convLastMsg[conversationId] || ts > convLastMsg[conversationId].ts) {
            convLastMsg[conversationId] = {
                ts,
                preview: (content.text || content.type || '').slice(0, 120)
            };
        }
        if (!isFromMe) {
            if (!convLastInbound[conversationId] || ts > convLastInbound[conversationId]) {
                convLastInbound[conversationId] = ts;
            }
        }

        messagePayloads.push({
            whatsapp_message_id: msg.key.id,
            sasa_business_id:    businessId,
            contact_id:          contactId,
            conversation_id:     conversationId,
            direction:           isFromMe ? 'out' : 'in',
            role:                isFromMe ? 'admin' : 'user',
            agent_role:          isFromMe ? 'human' : 'legacy_ai',
            type:                content.type,
            content:             content,
            created_at:          new Date(ts * 1000).toISOString(),
            status:              'sent',
            is_read:             isFromMe,
            raw_payload:         msg
        });
    }

    for (let i = 0; i < messagePayloads.length; i += 100) {
        const chunk = messagePayloads.slice(i, i + 100);
        const { error } = await supabase
            .from('messages')
            .upsert(chunk, { onConflict: 'whatsapp_message_id' });
        if (error) {
            console.error(`  [Messages] Chunk ${Math.floor(i / 100) + 1} error: ${error.message}`);
        } else {
            stats.messages += chunk.length;
        }
    }
    console.log(`  [Messages] ${stats.messages} upserted, ${stats.skipped} skipped`);

    // ── Update conversation metadata ──────────────────────────────
    let metaUpdated = 0;
    for (const [convId, data] of Object.entries(convLastMsg)) {
        const fields = { last_message_preview: data.preview };
        if (convLastInbound[convId]) {
            fields.last_user_message_at = new Date(convLastInbound[convId] * 1000).toISOString();
        }
        const { error } = await supabase.from('conversations').update(fields).eq('id', convId);
        if (!error) metaUpdated++;
    }
    console.log(`  [Conversations] ${metaUpdated} metadata rows updated`);

    return stats;
}

// ==========================================
// 5. LIVE MESSAGE HANDLER (messages.upsert)
// ==========================================

async function processLiveMessage(payload, businessId) {
    // Evolution sends a single message object in data for live messages
    // Normalise to array to handle both formats defensively
    let rawMessages = [];
    if (Array.isArray(payload.data)) {
        rawMessages = payload.data;
    } else if (payload.data?.key) {
        rawMessages = [payload.data];
    } else if (payload.data?.messages) {
        rawMessages = payload.data.messages;
    } else {
        console.log('  [Live] No parseable messages in payload');
        return;
    }

    for (const msg of rawMessages) {
        try {
            const jid = msg.key?.remoteJid;

            // ── Guards ───────────────────────────────────────────
            if (!jid || isGroupOrBroadcast(jid)) continue;
            if (msg.messageStubType) continue;   // system event, not a real message
            if (!msg.key?.id) continue;

            const isFromMe = msg.key.fromMe === true;
            const pushName = msg.pushName || null;
            const ts       = msg.messageTimestamp || Math.floor(Date.now() / 1000);
            const timestamp = new Date(ts * 1000).toISOString();

            // ── Get or create contact + conversation ─────────────
            const contact        = await getOrCreateContact(businessId, jid, pushName);
            const contactId      = contact.id;
            const conversationId = await getOrCreateConversation(businessId, contactId, jid);

            // ── Extract content ──────────────────────────────────
            const content = extractMessageContent(msg);
            if (!content) {
                console.log(`  [Live] Unhandled message type from ${jid} — skipping`);
                continue;
            }

            // ── Insert message ───────────────────────────────────
            // unread_count is managed by your existing DB trigger
            const { error: msgError } = await supabase
                .from('messages')
                .upsert({
                    whatsapp_message_id: msg.key.id,
                    sasa_business_id:    businessId,
                    contact_id:          contactId,
                    conversation_id:     conversationId,
                    direction:           isFromMe ? 'out' : 'in',
                    role:                isFromMe ? 'admin' : 'user',
                    agent_role:          isFromMe ? 'human' : 'legacy_ai',
                    type:                content.type,
                    content:             content,
                    created_at:          timestamp,
                    status:              'sent',
                    is_read:             isFromMe,
                    raw_payload:         msg
                }, { onConflict: 'whatsapp_message_id' });

            if (msgError) {
                console.error(`  [Live] Message insert error: ${msgError.message}`);
                continue;
            }

            // ── Update conversation metadata ─────────────────────
            const preview     = (content.text || content.type || '').slice(0, 120);
            const convUpdate  = {
                last_message_preview: preview,
                status:               'open'
            };
            if (!isFromMe) {
                convUpdate.last_user_message_at = timestamp;
            }

            await supabase
                .from('conversations')
                .update(convUpdate)
                .eq('id', conversationId);

            // ── Inbound-only: state machine + queue cleanup ──────
            if (!isFromMe) {
                await updateLeadStateOnReply(contactId, contact.lead_state);
                await cancelPendingFollowUps(contactId);

                // ── Consent response detection ───────────────────
                await processConsentResponse(contactId, content.text);

                // AI routing goes here in Phase 4
                // For now: message is in the DB, state is updated, queue is cleared
            }

            console.log(
                `  [Live] ✓ ${isFromMe ? 'OUT' : 'IN '} | ` +
                `type:${content.type.padEnd(12)} | ` +
                `contact:${contactId} | ` +
                `conv:${conversationId}`
            );

        } catch (msgErr) {
            console.error(`  [Live] Error processing message: ${msgErr.message}`);
            // Continue to next message — one bad message must not break the loop
        }
    }
}

// ==========================================
// 6. CONNECTION UPDATE HANDLER
// ==========================================

async function processConnectionUpdate(payload, businessId) {
    const state        = payload.data?.state;
    const statusReason = payload.data?.statusReason;

    console.log(`  [Connection] ${businessId} → state: ${state} (reason: ${statusReason || 'n/a'})`);

    if (state === 'open') {
        // WhatsApp successfully connected
        // Evolution will automatically fire messaging-history.set next
        // which processHistorySync will handle
        console.log(`  [Connection] ✓ WhatsApp connected for ${businessId}`);

        // Store connection timestamp in ai_config
        try {
            const { data: biz } = await supabase
                .from('businesses')
                .select('ai_config')
                .eq('sasa_business_id', businessId)
                .single();

            const updatedConfig = {
                ...(biz?.ai_config || {}),
                whatsapp_status:   'connected',
                whatsapp_connected_at: new Date().toISOString()
            };

            await supabase
                .from('businesses')
                .update({ ai_config: updatedConfig })
                .eq('sasa_business_id', businessId);

        } catch (e) {
            console.error(`  [Connection] Failed to update ai_config: ${e.message}`);
        }
    }

    if (state === 'close') {
        console.log(`  [Connection] ✗ WhatsApp disconnected for ${businessId}`);

        try {
            const { data: biz } = await supabase
                .from('businesses')
                .select('ai_config')
                .eq('sasa_business_id', businessId)
                .single();

            const updatedConfig = {
                ...(biz?.ai_config || {}),
                whatsapp_status:        'disconnected',
                whatsapp_disconnected_at: new Date().toISOString()
            };

            await supabase
                .from('businesses')
                .update({ ai_config: updatedConfig })
                .eq('sasa_business_id', businessId);

        } catch (e) {
            console.error(`  [Connection] Failed to update ai_config: ${e.message}`);
        }
    }
}

// ==========================================
// 7. EXPRESS ROUTES
// ==========================================

app.post('/webhook/evolution', async (req, res) => {
    const businessId = req.query.business_id;
    const eventType  = req.body?.event;

    if (!businessId) {
        return res.status(400).json({ error: 'Missing business_id query param' });
    }

    // Always respond 200 immediately — Evolution API will retry if we don't
    res.status(200).send('OK');

    console.log(`\n[Webhook] event:${eventType} | business:${businessId}`);

    try {
        switch (eventType) {

            case 'messaging-history.set':
                // Bulk historical import — fires once after WhatsApp connects
                await processHistorySync(req.body, businessId);
                console.log(`[History] ✓ Sync complete for ${businessId}`);
                await triggerPersonaPackGeneration(businessId);
                break;

            case 'messages.upsert':
                // Live message arriving in real time
                await processLiveMessage(req.body, businessId);
                break;

            case 'connection.update':
                // WhatsApp connection state changed
                await processConnectionUpdate(req.body, businessId);
                break;

            default:
                // Log unhandled events — useful for debugging, never crash on them
                console.log(`[Webhook] Unhandled event type: ${eventType}`);
        }
    } catch (err) {
        console.error(`[Webhook] Error handling ${eventType} for ${businessId}: ${err.message}`);
    }
});

app.get('/health', (_req, res) => res.json({ status: 'ok', service: 'whatsapp-cleaner' }));

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`WhatsApp cleaner service online — port ${PORT}`));
