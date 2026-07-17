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

### Pilot Ops roles

The existing `PILOT_ACCESS_CODE` remains the Commander login. Add a long random `PILOT_SESSION_SECRET` in Render to sign pilot sessions. Optional role-specific codes can be added without changing the current Commander code:

```text
PILOT_COMMANDER_NAME=Antoni Garcia
PILOT_SECONDARY_CODE=<secondary pilot private code>
PILOT_SECONDARY_NAME=Saul Garcia
PILOT_DISPATCH_CODE=<dispatch private code>
PILOT_DISPATCH_NAME=Flight Dispatch
PILOT_SESSION_SECRET=<long random secret>
```

Commander and Secondary sessions can record the locked `OUT`, `OFF`, `ON`, and `IN` movement times. Only the Commander session can acknowledge an OFP release. Pilot Ops records the active role beside new operational timeline entries.

### Face ID / Touch ID pilot access

Pilot Ops supports device passkeys. Sign in once with the pilot access code, open **Face ID / Touch ID device access** near the bottom of Pilot Ops, then choose **Set up this device**. Your phone or computer uses Face ID, Touch ID, or its device PIN; no biometric data is sent to or stored by NGA Private Aviation.

The pilot access code remains the fallback for a new device. Device passkeys can be removed from the same Pilot Ops panel. The custom HTTPS domain is required. With the current domain, no additional Render environment variables are required. If the public domain changes later, set these values in Render and redeploy:

```text
WEBAUTHN_RP_ID=ngaprivateaviation.com
WEBAUTHN_ORIGIN=https://ngaprivateaviation.com
```

### Booking emails

To automatically notify operations and send the booking contact a request receipt without paid SMTP, deploy a Google Apps Script email relay that sends through the Gmail account. Add these Render environment variables:

```text
GOOGLE_APPS_SCRIPT_EMAIL_URL=<Apps Script web app URL>
GOOGLE_APPS_SCRIPT_EMAIL_SECRET=<long private shared secret>
PILOT_NOTIFICATION_EMAIL=info.ngaprivateaviation@gmail.com
```

When set, these optional public document links are included in the automatic flight-confirmation email sent after pilot approval:

```text
BOOKING_AGREEMENT_URL=<public Private Flight Agreement URL>
BOOKING_REIMBURSEMENT_URL=<public Reimbursement Statement URL>
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
