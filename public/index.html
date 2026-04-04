<!doctype html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>NatGlobe Aviation / ACARS Weather</title>
  <meta name="apple-mobile-web-app-capable" content="yes" />
  <style>
    :root {
      --bg: #000000;
      --fg: #f2f2f2;
      --muted: #cfcfcf;
      --line: #ffffff;
      --button-bg: #d0d0d0;
      --button-fg: #000000;
    }

    * {
      box-sizing: border-box;
    }

    html, body {
      margin: 0;
      min-height: 100%;
      background: var(--bg);
      color: var(--fg);
      font-family: "Courier New", Courier, monospace;
    }

    body {
      display: flex;
      justify-content: center;
      align-items: flex-start;
      padding: 44px 20px;
    }

    .frame {
      width: min(980px, 100%);
      border: 2px solid var(--line);
      padding: 26px 24px 22px;
      background: #000;
    }

    .title {
      font-size: clamp(22px, 3vw, 34px);
      font-weight: 700;
      letter-spacing: 1px;
      line-height: 1.2;
      color: var(--muted);
      margin-bottom: 18px;
      text-transform: uppercase;
    }

    .intro {
      color: #bdbdbd;
      font-size: clamp(13px, 1.5vw, 18px);
      line-height: 1.35;
      max-width: 820px;
      margin-bottom: 22px;
    }

    .controls {
      display: grid;
      grid-template-columns: 1.2fr 0.85fr 0.85fr 0.75fr;
      gap: 12px;
      margin-bottom: 18px;
    }

    .icao,
    .btn {
      min-height: 58px;
      border: 2px solid var(--line);
      font-family: "Courier New", Courier, monospace;
      font-size: clamp(16px, 1.9vw, 28px);
    }

    .icao {
      background: #000;
      color: #fff;
      padding: 10px 14px;
      text-transform: uppercase;
      outline: none;
    }

    .btn {
      background: #000;
      color: #fff;
      cursor: pointer;
      font-weight: 700;
      padding: 10px 12px;
    }

    .btn.primary {
      background: var(--button-bg);
      color: var(--button-fg);
    }

    .status-box {
      border: 1px solid var(--line);
      min-height: 56px;
      display: flex;
      align-items: center;
      padding: 0 14px;
      margin-bottom: 16px;
      color: #d7d7d7;
      font-size: clamp(14px, 1.4vw, 22px);
      white-space: pre-wrap;
    }

    .preview-box {
      border: 2px solid var(--line);
      padding: 16px;
      min-height: 420px;
      margin-bottom: 16px;
      overflow: auto;
      background: #000;
    }

    .preview-text {
      margin: 0;
      color: #fff;
      font-family: "Courier New", Courier, monospace;
      font-size: clamp(15px, 1.35vw, 24px);
      line-height: 1.35;
      white-space: pre-wrap;
      word-break: break-word;
    }

    .footer-tip {
      color: #9f9f9f;
      font-size: clamp(12px, 1.15vw, 18px);
      line-height: 1.35;
    }

    @media (max-width: 900px) {
      .controls {
        grid-template-columns: 1fr;
      }

      .preview-box {
        min-height: 320px;
      }
    }
  </style>
</head>
<body>
  <main class="frame">
    <div class="title">NATGLOBE AVIATION / ACARS WEATHER</div>

    <div class="intro">
      Enter an ICAO code. Tap one button to fetch METAR/TAF, fall back to nearby
      airfields, and download a print-ready PNG. If offline, the last saved report on
      this device is used.
    </div>

    <div class="controls">
      <input
        id="icao"
        class="icao"
        type="text"
        maxlength="4"
        value="EFHK"
        placeholder="EFHK"
        autocapitalize="characters"
        autocorrect="off"
        spellcheck="false"
      />
      <button id="generateBtn" class="btn primary">Generate PNG</button>
      <button id="latestBtn" class="btn">Use Latest Saved</button>
      <button id="openBtn" class="btn">Open PNG</button>
    </div>

    <div id="statusBox" class="status-box">READY</div>

    <div class="preview-box">
      <pre id="previewText" class="preview-text">ACARS WEATHER REPORT
--------------------
TIME (UTC):
AIRPORT:

METAR:

TAF:

END OF REPORT</pre>
    </div>

    <div class="footer-tip">
      Tip on iPad: if the file opens in a new tab instead of downloading, use Share
      → Print or Save to Files.
    </div>
  </main>

  <script>
    async function clearOldCaches() {
      if ('serviceWorker' in navigator) {
        const regs = await navigator.serviceWorker.getRegistrations();
        for (const reg of regs) {
          await reg.unregister();
        }
      }

      if ('caches' in window) {
        const keys = await caches.keys();
        await Promise.all(keys.map(key => caches.delete(key)));
      }
    }

    const icaoInput = document.getElementById('icao');
    const generateBtn = document.getElementById('generateBtn');
    const latestBtn = document.getElementById('latestBtn');
    const openBtn = document.getElementById('openBtn');
    const statusBox = document.getElementById('statusBox');
    const previewText = document.getElementById('previewText');

    let latestBlobUrl = null;
    let latestIcao = null;

    function setStatus(text) {
      statusBox.textContent = text;
    }

    function saveLatest(icao, blob, reportText) {
      const reader = new FileReader();
      reader.onloadend = () => {
        localStorage.setItem(`ng_latest_png_${icao}`, reader.result);
        localStorage.setItem(`ng_latest_report_${icao}`, reportText);
        localStorage.setItem(`ng_last_icao`, icao);
      };
      reader.readAsDataURL(blob);
    }

    function loadLatest(icao) {
      const png = localStorage.getItem(`ng_latest_png_${icao}`);
      const report = localStorage.getItem(`ng_latest_report_${icao}`);
      return { png, report };
    }

    function showReport(report) {
      previewText.textContent = report;
    }

    async function blobToText(blob) {
      const url = URL.createObjectURL(blob);
      latestBlobUrl = url;
      latestIcao = icaoInput.value.trim().toUpperCase();
      return url;
    }

    async function fetchReportText(icao) {
      const res = await fetch(`/api/report-text?icao=${icao}&t=${Date.now()}`, {
        cache: 'no-store'
      });
      if (!res.ok) throw new Error(`TEXT HTTP ${res.status}`);
      return await res.text();
    }

    async function generate() {
      const icao = icaoInput.value.trim().toUpperCase();

      if (!/^[A-Z]{4}$/.test(icao)) {
        setStatus('INVALID ICAO');
        return;
      }

      setStatus(`FETCHING — ${icao}`);
      previewText.textContent = '';

      try {
        const [pngRes, reportText] = await Promise.all([
          fetch(`/api/weather-v2?icao=${icao}&t=${Date.now()}`, { cache: 'no-store' }),
          fetchReportText(icao)
        ]);

        if (!pngRes.ok) throw new Error(`PNG HTTP ${pngRes.status}`);

        const blob = await pngRes.blob();
        if (!blob || blob.size === 0) throw new Error('EMPTY PNG');

        const url = await blobToText(blob);

        showReport(reportText);
        saveLatest(icao, blob, reportText);
        setStatus(`DONE — ${icao}_ACARS_WEATHER.png`);

        generateBtn.dataset.url = url;
        openBtn.dataset.url = url;
      } catch (err) {
        console.error(err);
        const saved = loadLatest(icao);

        if (saved.report) {
          showReport(saved.report);
          if (saved.png) {
            generateBtn.dataset.url = saved.png;
            openBtn.dataset.url = saved.png;
          }
          setStatus(`OFFLINE OR API UNAVAILABLE — USING SAVED LOCAL REPORT`);
        } else {
          setStatus('ERROR FETCHING WEATHER');
        }
      }
    }

    generateBtn.addEventListener('click', async () => {
      await generate();

      const url = generateBtn.dataset.url;
      const icao = icaoInput.value.trim().toUpperCase();
      if (url) {
        const a = document.createElement('a');
        a.href = url;
        a.download = `${icao}_ACARS_WEATHER.png`;
        document.body.appendChild(a);
        a.click();
        a.remove();
      }
    });

    latestBtn.addEventListener('click', () => {
      const icao = icaoInput.value.trim().toUpperCase() || localStorage.getItem('ng_last_icao');
      if (!icao) {
        setStatus('NO SAVED REPORT');
        return;
      }

      const saved = loadLatest(icao);
      if (!saved.report) {
        setStatus('NO SAVED REPORT');
        return;
      }

      showReport(saved.report);
      generateBtn.dataset.url = saved.png || '';
      openBtn.dataset.url = saved.png || '';
      setStatus(`DONE — ${icao}_ACARS_WEATHER.png`);
    });

    openBtn.addEventListener('click', () => {
      const url = openBtn.dataset.url;
      if (!url) {
        setStatus('NO PNG READY');
        return;
      }
      window.open(url, '_blank');
    });

    (async () => {
      await clearOldCaches();
      const lastIcao = localStorage.getItem('ng_last_icao');
      if (lastIcao) icaoInput.value = lastIcao;
    })();
  </script>
</body>
</html>
