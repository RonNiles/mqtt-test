Here's a breakdown by group:

---

**CSS custom properties**
```css
:root { --blue: #007aff; --green: #4bd964; --disabled: #9ba2b5; }
```
Defines three named colours used throughout — blue for "off", green for "on", grey for "disconnected" or loading.

---

**Global reset / page layout**
```css
* { box-sizing: border-box; }
html, body { min-height: 100vh; }
body { margin: 0; display: flex; justify-content: center; align-items: center; }
```
`border-box` makes padding/border not add to element dimensions. The body is a full-height flex container that centres the switch both horizontally and vertically.

---

**`.switch` — the pill-shaped track**
```css
position: relative; width: 4rem; height: 2rem; border-radius: 1.5rem;
```
A rounded pill (4×2 rem). `position: relative` is needed so the absolutely-positioned knob inside can be placed with `left`.

The hidden checkbox (`opacity:0; top:-20px; pointer-events:none`) is kept in the DOM purely so `:focus`, `:checked`, and `:indeterminate` pseudo-classes can be read by CSS selectors — it is never visible.

`.switch.loading` dims the whole widget and blocks pointer events while a state change is in flight.

---

**`.toggle` — the circular knob**
```css
position: absolute; width: 2rem; height: 2rem; border-radius: 50%;
transition: width 250ms ease-out, left 250ms ease-out;
```
A circle that slides along the track. The `transition` gives it the smooth slide whenever `left` or `width` changes.

---

**State-driven knob position & colour** (via `data-state` attribute on `.switch`)

| State | `left` | colour |
|---|---|---|
| `off` | `0` (left edge) | `--blue` |
| `on` | `calc(100% - 2rem)` (right edge) | `--green` |
| `disconnected` | `calc(50% - 1rem)` (centre) | `--disabled` |

The JavaScript sets `data-state` on the label element, and these selectors react instantly.

---

**Loading overrides**
While `.loading` is present the knob colour is forced to `--disabled` in all positions, and the `:focus` outline is also changed to grey — visually signalling that input is blocked.

---

**Focus outlines** (`:focus-within`)
When the hidden checkbox receives keyboard focus, a coloured outline ring appears around the knob, matching the current state's colour. This preserves keyboard accessibility without showing the raw checkbox.

---

**`.spinner` — the loading indicator**
A circle made by making one quarter of a thick border transparent and spinning it with the `rotate` keyframe (`transform: rotate` 0→360°). It sits hidden (`opacity:0`) inside the knob at all times; `.switch.loading .spinner` makes it visible whenever loading is active.
