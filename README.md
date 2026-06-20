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

## Local run

```bash
npm install
npm start
```

Open `http://localhost:3000`
