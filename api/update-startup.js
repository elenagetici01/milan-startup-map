// Vercel serverless function — aggiorna logo/notes di una startup in startups.json
//
// Sicurezza:
//   - Richiede header X-Edit-Secret che deve corrispondere a process.env.EDIT_SECRET
//   - Rate limiting: max 10 tentativi ogni 15 minuti per IP (deterrente brute force)
//   - GITHUB_TOKEN mai esposto al frontend

const attempts = new Map(); // { ip: [timestamps] }

function isRateLimited(ip) {
  const now = Date.now();
  const window = 15 * 60 * 1000; // 15 minuti
  const max = 10;
  const list = (attempts.get(ip) || []).filter(t => now - t < window);
  list.push(now);
  attempts.set(ip, list);
  return list.length > max;
}

module.exports = async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, X-Edit-Secret');
  if (req.method === 'OPTIONS') return res.status(200).end();
  if (req.method !== 'POST') return res.status(405).json({ error: 'Method not allowed' });

  // Rate limiting
  const ip = req.headers['x-forwarded-for']?.split(',')[0] || req.socket.remoteAddress || 'unknown';
  if (isRateLimited(ip)) {
    return res.status(429).json({ error: 'Troppi tentativi. Riprova tra 15 minuti.' });
  }

  // Autenticazione
  const secret = req.headers['x-edit-secret'];
  if (!secret || secret !== process.env.EDIT_SECRET) {
    return res.status(401).json({ error: 'Password errata.' });
  }

  const { id, logo, notes } = req.body;
  if (id === undefined) return res.status(400).json({ error: 'Missing id' });

  const token = process.env.GITHUB_TOKEN;
  if (!token) return res.status(500).json({ error: 'GITHUB_TOKEN non configurato' });

  const API = 'https://api.github.com/repos/elenagetici01/milan-startup-map/contents/startups.json';
  const ghHeaders = {
    Authorization: `Bearer ${token}`,
    Accept: 'application/vnd.github+json',
    'X-GitHub-Api-Version': '2022-11-28',
    'Content-Type': 'application/json',
  };

  // Retry fino a 3 volte per conflitti di scrittura concorrente
  for (let attempt = 1; attempt <= 3; attempt++) {
    try {
      const fetchRes = await fetch(API, { headers: ghHeaders });
      if (!fetchRes.ok) return res.status(502).json({ error: 'Errore fetch GitHub' });
      const fileData = await fetchRes.json();

      const startups = JSON.parse(Buffer.from(fileData.content, 'base64').toString('utf-8'));
      const startup  = startups.find(s => s.id === Number(id));
      if (!startup) return res.status(404).json({ error: `Startup id ${id} non trovata` });

      startup.logo  = logo  !== undefined ? (logo  || null) : (startup.logo  ?? null);
      startup.notes = notes !== undefined ? (notes || '')   : (startup.notes ?? '');

      const newContent = Buffer.from(JSON.stringify(startups, null, 2), 'utf-8').toString('base64');
      const pushRes = await fetch(API, {
        method: 'PUT',
        headers: ghHeaders,
        body: JSON.stringify({
          message: `data: update logo/notes — id ${id}`,
          content: newContent,
          sha: fileData.sha,
          branch: 'main',
        }),
      });

      if (pushRes.status === 409 && attempt < 3) {
        await new Promise(r => setTimeout(r, 400 * attempt));
        continue;
      }
      if (!pushRes.ok) {
        const err = await pushRes.json().catch(() => ({}));
        return res.status(502).json({ error: err.message || 'GitHub push failed' });
      }

      return res.status(200).json({ success: true });

    } catch (err) {
      if (attempt === 3) return res.status(500).json({ error: err.message });
      await new Promise(r => setTimeout(r, 400 * attempt));
    }
  }
};
