# NatGlobe Aviation ACARS Weather

iPad-friendly ACARS-style METAR/TAF weather report generator with nearby-airfield fallback, PNG export, server cache, and local offline fallback.

## Deploy on Render

1. Create a GitHub repo and upload these files.
2. In Render, create a new Web Service from that repo.
3. Build command: `npm install`
4. Start command: `npm start`
5. Once deployed, open the Render URL on your iPad.

### Booking database

The booking system uses temporary memory until a PostgreSQL connection is configured. For real bookings, create a Render Postgres database and add its internal connection string to the web service as `DATABASE_URL`. On the next deploy the app creates its booking request, availability, and operations timeline tables automatically.

Do not use the temporary mode for production passenger or identity data: Render restarts clear it.

### Booking emails

To automatically notify operations and send the booking contact a request receipt, create a verified sender in [Resend](https://resend.com/) and add these Render environment variables:

```text
RESEND_API_KEY=<your Resend API key>
BOOKING_EMAIL_FROM=Private Flight <bookings@your-verified-domain.com>
BOOKING_EMAIL_REPLY_TO=info.ngaprivateaviation@gmail.com
PILOT_NOTIFICATION_EMAIL=operations@example.com
```

If the custom domain sender is not ready, Gmail can be used temporarily instead. Enable two-step verification on the Gmail account, create a Google App Password for Mail, then add these Render environment variables. Gmail takes priority when these values are present; remove them later to return to Resend.

```text
GMAIL_SMTP_USER=info.ngaprivateaviation@gmail.com
GMAIL_SMTP_APP_PASSWORD=<Google App Password>
GMAIL_SMTP_FROM=NGA Private Aviation <info.ngaprivateaviation@gmail.com>
```

The booking contact receives a receipt with the reference, route, requested departure, and passenger count. Operations receives a separate alert. Neither email includes passport, medical, identity, emergency, or signature data.

### Web boarding passes

After a pilot approves a booking, Booking Ops can issue one private web boarding-pass link for each passenger. The link opens a mobile-friendly ticket with a QR code and only shows trip details needed to board. Passport, date-of-birth, medical, contact, and emergency data are never included on the public pass.

The ticket embeds the `Computer Says No` font by Christian Munk under CC BY-SA 3.0. Its font file and accompanying licence are kept in `public/fonts/`.

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
