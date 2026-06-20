# NatGlobe Aviation ACARS Weather

iPad-friendly ACARS-style METAR/TAF weather report generator with nearby-airfield fallback, PNG export, server cache, and local offline fallback.

## Deploy on Render

1. Create a GitHub repo and upload these files.
2. In Render, create a new Web Service from that repo.
3. Build command: `npm install`
4. Start command: `npm start`
5. Once deployed, open the Render URL on your iPad.
6. In Safari, use **Share > Add to Home Screen**.

### Booking database

The booking system uses temporary memory until a PostgreSQL connection is configured. For real bookings, create a Render Postgres database and add its internal connection string to the web service as `DATABASE_URL`. On the next deploy the app creates its booking request, availability, and operations timeline tables automatically.

Do not use the temporary mode for production passenger or identity data: Render restarts clear it.

### Apple Wallet passes

Approved bookings can generate one Apple Wallet boarding pass per passenger from Booking Ops. Apple requires every `.pkpass` bundle to be signed with a Pass Type ID certificate. Create a Pass Type ID and its certificate in the Apple Developer account, then add these Render secret environment variables:

```text
WALLET_PASS_TYPE_IDENTIFIER=pass.com.your-company.privateflight
WALLET_TEAM_IDENTIFIER=YOUR_APPLE_TEAM_ID
WALLET_ORGANIZATION_NAME=NatGlobe Aviation
WALLET_SIGNER_CERT_BASE64=<base64 of Pass Type ID signing certificate PEM>
WALLET_SIGNER_KEY_BASE64=<base64 of matching private key PEM>
WALLET_SIGNER_KEY_PASSPHRASE=<private key passphrase, if used>
WALLET_WWDR_BASE64=<base64 of Apple WWDR intermediate certificate PEM>
```

Keep certificates and keys in Render secrets only. Never commit them or send them in chat. Wallet passes intentionally exclude passport, date-of-birth, medical, and other sensitive booking data.

## Local run

```bash
npm install
npm start
```

Open `http://localhost:3000`
