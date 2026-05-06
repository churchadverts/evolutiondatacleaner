import express from 'express';
import { createClient } from '@supabase/supabase-js';
import dotenv from 'dotenv';
import ws from 'ws'; // Add WebSocket import

dotenv.config();

const app = express();
app.use(express.json({ limit: '50mb' })); // Allow large history payloads

// Update the client initialization with WebSocket configuration
const supabase = createClient(
    process.env.SUPABASE_URL, 
    process.env.SUPABASE_SERVICE_KEY, 
    {
        auth: {
            persistSession: false // Good practice for server-side scripts
        },
        global: {
            headers: { 'x-my-custom-header': 'heysasa-cleaner' }
        },
        realtime: {
            websocket: ws // This satisfies the Node 20 requirement
        }
    }
);

app.post('/webhook/evolution', async (req, res) => {
    // 1. Extract Business ID from the Webhook URL Query
    const businessId = req.query.business_id;
    const eventType = req.body.event;

    if (!businessId) {
        return res.status(400).send("Missing business_id in webhook URL");
    }

    // Acknowledge receipt immediately so Evolution API doesn't retry
    res.status(200).send("OK");

    // We only want to process the heavy history dump here
    if (eventType === 'messaging-history.set') {
        try {
            await processHistorySync(req.body, businessId);
            console.log(`[Success] History synced for Business: ${businessId}`);
        } catch (error) {
            console.error(`[Error] History sync failed:`, error);
        }
    }
});

async function processHistorySync(payload, businessId) {
    const { contacts, chats, messages } = payload.data;
    
    // --- PHASE 1: UPSERT CONTACTS ---
    const contactPayloads = contacts.map(c => ({
        business_id: businessId,
        social_platform: 'whatsapp',
        social_id: c.id, // e.g., 2547... @s.whatsapp.net
        name: c.name || c.notify || 'Unknown',
        channel: 'whatsapp'
    }));

    // Upsert and return the internal IDs for the next phase
    const { data: insertedContacts, error: contactError } = await supabase
        .from('contacts')
        .upsert(contactPayloads, { onConflict: 'business_id, social_platform, social_id' })
        .select('id, social_id');

    if (contactError) throw contactError;

    // Create a map of JID -> internal contact_id (BigInt)
    const contactMap = insertedContacts.reduce((acc, curr) => {
        acc[curr.social_id] = curr.id;
        return acc;
    }, {});


    // --- PHASE 2: UPSERT CONVERSATIONS ---
    const conversationPayloads = chats.map(chat => ({
        business_id: businessId,
        contact_id: contactMap[chat.id], // Map using the dictionary we just made
        external_id: chat.id,
        channel: 'whatsapp',
        type: 'dm',
        status: chat.unreadCount > 0 ? 'open' : 'closed',
        unread_count: chat.unreadCount || 0
    })).filter(c => c.contact_id); // Ensure we only add convos for contacts we successfully saved

    const { data: insertedConvos, error: convoError } = await supabase
        .from('conversations')
        .upsert(conversationPayloads, { onConflict: 'business_id, contact_id' })
        .select('id, external_id');

    if (convoError) throw convoError;

    // Create a map of JID -> internal conversation_id (UUID)
    const convoMap = insertedConvos.reduce((acc, curr) => {
        acc[curr.external_id] = curr.id;
        return acc;
    }, {});


    // --- PHASE 3: UPSERT MESSAGES (Filtered to 3 Months) ---
    const threeMonthsAgo = Math.floor(Date.now() / 1000) - (90 * 24 * 60 * 60);

    const messagePayloads = messages
        .filter(msg => msg.messageTimestamp > threeMonthsAgo) // The Gatekeeper
        .map(msg => {
            const jid = msg.key.remoteJid;
            const textContent = msg.message?.conversation || msg.message?.extendedTextMessage?.text || "";
            
            return {
                whatsapp_message_id: msg.key.id,
                business_id: businessId,
                contact_id: contactMap[jid],
                conversation_id: convoMap[jid],
                direction: msg.key.fromMe ? 'out' : 'in',
                role: msg.key.fromMe ? 'ai' : 'user', // Basic assumption, can be updated later
                content: { text: textContent },
                created_at: new Date(msg.messageTimestamp * 1000).toISOString(),
                status: 'sent',
                is_read: msg.key.fromMe ? true : false,
                raw_payload: msg
            };
        })
        .filter(m => m.content.text !== "" && m.conversation_id); // Drop empty messages or unlinked ones

    // Batch insert messages in chunks of 100 to prevent Supabase timeouts
    for (let i = 0; i < messagePayloads.length; i += 100) {
        const chunk = messagePayloads.slice(i, i + 100);
        const { error: msgError } = await supabase
            .from('messages')
            .upsert(chunk, { onConflict: 'whatsapp_message_id' });
        
        if (msgError) console.error("Chunk upsert error:", msgError);
    }
}

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`Leapcell Webhook running on port ${PORT}`));
