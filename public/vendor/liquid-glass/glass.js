/* Refraction shader derived from Na2H2P2O7/liquid-glass-skill (MIT).
 * See LICENSE and NOTICE.md. Browser host: static app-owned image only. */
(() => {
    'use strict';
    const MAX_PANELS = 4;
    const FRAME_INTERVAL = 1000 / 30;
    const SHEET_WARP = 0.12;
    const SHEET_BLUR_CSS = 8.0;
    const PANEL_WARP = 0.30;
    const PANEL_BLUR_CSS = 2.5;
    // "Pressed glass": the pointer locally deforms the sheet, bending the
    // refraction underneath. Radius/strength of the press in css px.
    const PRESS_SIGMA = 50.0;
    const PRESS_BASE = 0.42;
    const PRESS_BOOST = 0.34;

    const VERT_SRC = `
attribute vec2 aPos;
void main() {
    gl_Position = vec4(aPos, 0.0, 1.0);
}`;

    const FRAG_SRC = `
#ifdef GL_FRAGMENT_PRECISION_HIGH
precision highp float;
#else
precision mediump float;
#endif

#define MAX_PANELS ${MAX_PANELS}

uniform sampler2D uTex;
uniform vec2 uRes;          // canvas size, physical px
uniform float uDpr;
uniform vec2 uWinPos;       // window top-left on screen, css px
uniform vec2 uScreen;       // screen size, css px
uniform float uImgAspect;   // wallpaper aspect (w/h)
uniform vec4 uClip;
uniform float uClipRadius;
uniform float uSheetRadius; // physical px
uniform vec4 uSheetTint;    // rgb + amount
uniform vec4 uPanelTint;    // rgb + amount
uniform float uRim;
uniform int uPanelCount;
uniform vec4 uPanelRect[MAX_PANELS];    // center.xy, halfSize.xy (physical, y-up)
uniform vec4 uPanelParams[MAX_PANELS];  // radius, warp, highlight, blur css px
uniform vec4 uPress;                    // pointer css pos (y-down), strength, press boost

float sdRoundRect(vec2 p, vec2 halfSize, float r) {
    vec2 q = abs(p) - halfSize + r;
    return length(max(q, 0.0)) + min(max(q.x, q.y), 0.0) - r;
}

float dither(vec2 p) {
    return fract(sin(dot(p, vec2(12.9898, 78.233))) * 43758.5453) - 0.5;
}

// Screen uv -> wallpaper uv with cover fit (wallpaper fills the screen).
vec2 coverUv(vec2 suv) {
    float sa = uScreen.x / uScreen.y;
    vec2 s = sa > uImgAspect ? vec2(1.0, uImgAspect / sa) : vec2(sa / uImgAspect, 1.0);
    return (suv - 0.5) * s + 0.5;
}

vec3 sampleWall(vec2 cssPos, float blurCss) {
    vec2 suv = (uWinPos + cssPos) / uScreen;
    vec2 st = vec2(blurCss) / uScreen;
    vec3 acc = vec3(0.0);
    for (int x = -1; x <= 1; x++) {
        for (int y = -1; y <= 1; y++) {
            acc += texture2D(uTex, coverUv(suv + vec2(float(x), float(y)) * st)).rgb;
        }
    }
    return acc / 9.0;
}

void main() {
    vec2 fragPx = gl_FragCoord.xy;
    vec2 cssPos = vec2(fragPx.x, uRes.y - fragPx.y) / uDpr; // css px, y-down
    vec2 halfRes = uRes * 0.5;

    float sdSheet = sdRoundRect(fragPx - halfRes, halfRes, uSheetRadius);
    float sdClip = sdRoundRect(fragPx - uClip.xy, uClip.zw, uClipRadius);
    float sheetMask = 1.0 - smoothstep(-0.75, 0.75, sdClip);
    if (sheetMask < 0.004) {
        gl_FragColor = vec4(0.0);
        return;
    }

    // Pressed glass: a gaussian bulge under the pointer bends the sampling
    // of the backdrop — the same refraction the sheet already does, locally
    // deepened, as if a fingertip were deforming the pane. Zero shift at the
    // press center, strongest pull in a ring around it.
    vec2 pv = cssPos - uPress.xy;
    float bulge = exp(-dot(pv, pv) / (2.0 * ${PRESS_SIGMA.toFixed(1)} * ${PRESS_SIGMA.toFixed(1)}));
    vec2 pressDisp = -pv * bulge * uPress.z *
        (${PRESS_BASE.toFixed(2)} + ${PRESS_BOOST.toFixed(2)} * uPress.w);

    // Window sheet: gentle lens warp near the window edge + frost.
    vec2 cssCenter = halfRes / uDpr;
    float innerS = clamp(-sdSheet / (26.0 * uDpr), 0.0, 1.0);
    float profS = (1.0 - innerS) * (1.0 - innerS);
    vec2 warpedS = cssCenter + (cssPos - cssCenter) * (1.0 - ${SHEET_WARP.toFixed(3)} * profS);
    vec3 col = sampleWall(warpedS + pressDisp, ${SHEET_BLUR_CSS.toFixed(1)});
    col = mix(col, uSheetTint.rgb, uSheetTint.a);
    float rimS = smoothstep(-2.5, -0.6, sdSheet) * (1.0 - smoothstep(-0.2, 0.75, sdSheet));
    col += vec3(1.0) * rimS * uRim;

    // Panels: thicker glass — stronger warp, more blur, extra tint, rim light.
    for (int i = 0; i < MAX_PANELS; i++) {
        if (i >= uPanelCount) break;

        vec2 center = uPanelRect[i].xy;
        vec2 halfSize = uPanelRect[i].zw;
        vec2 p = fragPx - center;
        float sd = sdRoundRect(p, halfSize, uPanelParams[i].x);
        if (sd < 1.0) {
            float body = (1.0 - smoothstep(-0.75, 0.75, sd));
            float highlight = uPanelParams[i].z;

            float edgeZone = min(min(halfSize.x, halfSize.y), 26.0 * uDpr) + 6.0 * uDpr;
            float inner = clamp(-sd / edgeZone, 0.0, 1.0);
            float prof = (1.0 - inner) * (1.0 - inner);
            float warp = uPanelParams[i].y * (1.0 + 0.6 * highlight) * prof;

            vec2 cCss = vec2(center.x, uRes.y - center.y) / uDpr;
            vec2 warped = cCss + (cssPos - cCss) * (1.0 - warp);

            // The press deforms the shared pane, so panels refract through
            // it too — slightly amplified, glass-on-glass.
            vec3 glass = sampleWall(warped + pressDisp * 1.4, uPanelParams[i].w);
            glass = mix(glass, uSheetTint.rgb, uSheetTint.a * 0.6);
            glass = mix(glass, uPanelTint.rgb, uPanelTint.a * (1.0 + 0.4 * highlight));

            float rim = smoothstep(-2.5, -0.6, sd) * (1.0 - smoothstep(-0.2, 0.75, sd));
            float topBias = 0.5 + 0.5 * clamp(p.y / max(halfSize.y, 1.0), -1.0, 1.0);
            glass += vec3(1.0) * rim * (uRim + 0.22 * highlight) * (0.4 + 0.6 * topBias);

            col = mix(col, glass, body);
        }
    }

    col += dither(fragPx) / 255.0;
    col = clamp(col, 0.0, 1.0);
    gl_FragColor = vec4(col * sheetMask, sheetMask); // premultiplied alpha
}`;


    // Keep the EFB host separate from the shader. No animation clock when idle.
    window.PilotGlassRenderer = function createRenderer(canvas, surfaces, controls, image) {
        const gl = canvas.getContext('webgl', { alpha:true, depth:false, stencil:false, antialias:false });
        if (!gl || gl.isContextLost()) return null;
        const program = gl.createProgram();
        for (const [type, source] of [[gl.VERTEX_SHADER, VERT_SRC], [gl.FRAGMENT_SHADER, FRAG_SRC]]) {
            const shader = gl.createShader(type);
            gl.shaderSource(shader, source);
            gl.compileShader(shader);
            if (!gl.getShaderParameter(shader, gl.COMPILE_STATUS)) { console.warn('Pilot glass shader:',gl.getShaderInfoLog(shader)); gl.deleteShader(shader); gl.deleteProgram(program); return null; }
            gl.attachShader(program, shader);
            gl.deleteShader(shader);
        }
        gl.linkProgram(program);
        if (!gl.getProgramParameter(program, gl.LINK_STATUS)) { gl.deleteProgram(program); return null; }
        gl.useProgram(program);
        const buffer = gl.createBuffer();
        gl.bindBuffer(gl.ARRAY_BUFFER, buffer);
        gl.bufferData(gl.ARRAY_BUFFER, new Float32Array([-1,-1,1,-1,-1,1,1,1]), gl.STATIC_DRAW);
        const pos = gl.getAttribLocation(program, 'aPos');
        gl.enableVertexAttribArray(pos);
        gl.vertexAttribPointer(pos, 2, gl.FLOAT, false, 0, 0);
        const tex = gl.createTexture();
        gl.bindTexture(gl.TEXTURE_2D, tex);
        gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_MIN_FILTER, gl.LINEAR);
        gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_MAG_FILTER, gl.LINEAR);
        for (const axis of [gl.TEXTURE_WRAP_S, gl.TEXTURE_WRAP_T]) gl.texParameteri(gl.TEXTURE_2D, axis, gl.CLAMP_TO_EDGE);
        // Rasterize our small SVG once. Direct SVG uploads vary across WebGL implementations.
        const pixels = document.createElement('canvas');
        pixels.width = image.naturalWidth; pixels.height = image.naturalHeight;
        pixels.getContext('2d').drawImage(image,0,0);
        gl.texImage2D(gl.TEXTURE_2D, 0, gl.RGBA, gl.RGBA, gl.UNSIGNED_BYTE, pixels);
        if (gl.getError() !== gl.NO_ERROR) { gl.deleteTexture(tex); gl.deleteBuffer(buffer); gl.deleteProgram(program); return null; }
        const u = {};
        for (const key of ['uTex','uRes','uDpr','uScreen','uWinPos','uImgAspect','uSheetRadius','uSheetTint','uPanelTint','uRim','uPress','uPanelCount','uPanelRect[0]','uPanelParams[0]','uClip','uClipRadius']) {
            u[key] = gl.getUniformLocation(program,key);
        }
        gl.uniform1i(u.uTex, 0);
        gl.uniform1f(u.uImgAspect, image.naturalWidth / image.naturalHeight);
        const rects = new Float32Array(MAX_PANELS * 4), params = new Float32Array(MAX_PANELS * 4);
        function geometry(el, dpr, h) {
            const r = el.getBoundingClientRect();
            return [dpr*(r.left+r.width/2), h-dpr*(r.top+r.height/2), dpr*r.width/2, dpr*r.height/2];
        }
        function radius(el, box, dpr) { return Math.min(parseFloat(getComputedStyle(el).borderRadius)||0,box[2]/dpr,box[3]/dpr)*dpr; }
        return {
            frameInterval: FRAME_INTERVAL,
            paint(night, press) {
                if (gl.isContextLost()) return false;
                const w = window.innerWidth, h = window.innerHeight;
                const dpr = Math.min(window.devicePixelRatio || 1, 1.25);
                const pw = Math.max(1,Math.round(w*dpr)), ph = Math.max(1,Math.round(h*dpr));
                if (canvas.width !== pw || canvas.height !== ph) { canvas.width=pw; canvas.height=ph; }
                gl.viewport(0,0,pw,ph);
                gl.uniform2f(u.uRes,pw,ph); gl.uniform1f(u.uDpr,dpr); gl.uniform2f(u.uScreen,w,h);
                gl.uniform4f(u.uSheetTint, ...(night ? [.065,.075,.075,.77] : [.97,.98,.975,.82]));
                gl.uniform4f(u.uPanelTint, ...(night ? [.10,.12,.12,.54] : [.95,.965,.96,.64]));
                gl.uniform1f(u.uRim, night ? .13 : .2);
                gl.uniform4f(u.uPress,press.x,press.y,press.amount,press.amount);
                const panels = controls.slice(0, MAX_PANELS);
                panels.forEach((el,i) => {
                    const box = geometry(el,dpr,ph);
                    rects.set(box,i*4); params.set([radius(el,box,dpr),PANEL_WARP,.45*press.amount,PANEL_BLUR_CSS],i*4);
                });
                gl.uniform1i(u.uPanelCount, panels.length);
                gl.uniform4fv(u['uPanelRect[0]'], rects); gl.uniform4fv(u['uPanelParams[0]'], params);
                gl.disable(gl.SCISSOR_TEST); gl.clearColor(0,0,0,0); gl.clear(gl.COLOR_BUFFER_BIT);
                gl.enable(gl.SCISSOR_TEST);
                for (const el of surfaces) {
                    const box = geometry(el,dpr,ph);
                    if (box[2]<1 || box[3]<1) continue;
                    gl.scissor(Math.floor(box[0]-box[2]),Math.floor(box[1]-box[3]),Math.ceil(2*box[2])+1,Math.ceil(2*box[3])+1);
                    gl.uniform4fv(u.uClip,box); gl.uniform1f(u.uClipRadius,radius(el,box,dpr));
                    gl.drawArrays(gl.TRIANGLE_STRIP,0,4);
                }
                gl.disable(gl.SCISSOR_TEST);
                return true;
            }
        };
    };
})();
