import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import vm from 'node:vm';

const read = path => fs.readFileSync(new URL('../' + path, import.meta.url), 'utf8');
function harness(compact) {
  const nodes = new Map(), classes = new Set(), properties = new Map();
  const node = id => {
    if (!nodes.has(id)) nodes.set(id, {
      value:'', events:{}, attributes:{},
      addEventListener(type, handler) { this.events[type] = handler; },
      setAttribute(name, value) { this.attributes[name] = value; },
      querySelector: selector => node(id + selector),
      getBoundingClientRect: () => ({ height:92 }),
      focus() {}, showModal() {}, close() {}
    });
    return nodes.get(id);
  };
  const media = { matches:compact, addEventListener(type, handler) { this.change = handler; } };
  let observed, resize;
  class ResizeObserver {
    constructor(handler) { resize = handler; }
    observe(target) { observed = target; }
  }
  const document = {
    getElementById:node, querySelector:node, querySelectorAll:() => [], createElement:node,
    documentElement:{ style:{ setProperty:(name, value) => properties.set(name, value) } },
    body:{ append() {}, classList:{
      contains:name => classes.has(name),
      toggle(name, active) { active ? classes.add(name) : classes.delete(name); }
    } }
  };
  const window = { matchMedia:() => media, ResizeObserver };
  vm.runInNewContext(read('public/pilot-ofp-touch.js'), { document, window, ResizeObserver });
  return { node, media, classes, properties, resize, observed };
}

test('portrait OFP starts with a collapsed rail and correct accessible toggle', () => {
  const h = harness(true);
  assert.ok(h.classes.has('ofp-sections-collapsed'));
  assert.equal(h.node('ofpSectionsToggle').attributes['aria-expanded'], 'false');
  assert.equal(h.node('ofpSectionsToggle').attributes['aria-label'], 'Show flight sections');
});

test('automatic rail layout follows rotation until the pilot makes a choice', () => {
  const h = harness(false);
  assert.equal(h.classes.size, 0);
  h.media.change({ matches:true });
  assert.ok(h.classes.has('ofp-sections-collapsed'));
  h.node('ofpSectionsToggle').events.click();
  assert.equal(h.node('ofpSectionsToggle').attributes['aria-expanded'], 'true');
  h.media.change({ matches:true });
  assert.equal(h.classes.size, 0, 'rotation must preserve the pilot’s explicit choice');
  h.node('ofpSectionsToggle').events.click();
  assert.ok(h.classes.has('ofp-sections-collapsed'));
});

test('editor scroll clearance follows the rendered toolbar height', () => {
  const h = harness(false);
  assert.equal(h.observed, h.node('.folder-toolbar'));
  h.resize();
  assert.equal(h.properties.get('--ofp-toolbar-height'), '92px');
});

test('layout contracts prevent legacy body spacing and fixed filter widths returning', () => {
  const shell = read('public/pilot-app-shell.css');
  const legacy = read('public/pilot-secondary-reference.css');
  assert.match(shell, /html body\.pilot-ops-v2 \{ margin:0; padding:0; \}/);
  assert.doesNotMatch(shell, /\.efb-masthead \{ --shell-header:/);
  assert.match(legacy, /#bookingsWorkspace \.quick-filters \.quick-filter \{/);
  assert.doesNotMatch(legacy, /#bookingsWorkspace \.quick-filter \{[^}]*min-width:/);
});

test('Home empty state and safety notice stay outside the absolute map stage', () => {
  const html = read('views/booking-ops.html');
  const stage = html.indexOf('<div class="ops-globe-stage">');
  const welcome = html.indexOf('id="cockpitEmptyFlight"');
  const note = html.indexOf('<span class="ops-map-note">');
  assert.ok(welcome < stage && stage < note);
  assert.match(html.slice(stage, note), /<\/div><\/div>\s*$/);
  assert.doesNotMatch(read('public/pilot-premium.css'), /\.cockpit-empty-flight \{ position:absolute/);
});

test('OFP avoids duplicate aircraft controls and responds to available editor width', () => {
  const html = read('views/operational-flight-plan.html');
  const css = read('public/pilot-ofp-touch.css');
  assert.match(html, /aircraft-profile-select" data-native-select data-field="aircraftRegistration"/);
  assert.match(css, /\.shell:not\(\[data-workspace=SUMMARY\]\) \.ofp-planning-segments \{ display:none; \}/);
  assert.match(css, /container-type:inline-size/);
  assert.match(css, /@container \(max-width:700px\)/);
  assert.match(css, /@container \(max-width:460px\)/);
});
