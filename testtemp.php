<!DOCTYPE html>
<html>
<head>
  <meta http-equiv="content-type" content="text/html; charset=UTF-8">
  <meta name="robots" content="noindex, nofollow">
  <meta name="googlebot" content="noindex, nofollow">
  <!--meta http-equiv="refresh" content="5"-->

  <title>Temperature Update</title>
  <style>
    :root {
      --pump-green: #4bd964;
      --pump-orange: #ff9500;
      --pump-disabled: #9ba2b5;
    }

    .pump-controls {
      display: flex;
      flex-direction: column;
      align-items: flex-start;
      gap: 1rem;
      padding: 1rem 0;
    }

    .pump-switch-row {
      display: flex;
      flex-direction: column;
      justify-content: center;
      align-items: center;
      gap: 0.75rem;
      width: 200px;
      height: 150px;
      border: 1px solid #bfc5d1;
      background: #fff;
    }

    .pump-switch {
      position: relative;
      display: flex;
      align-items: center;
      width: 8rem;
      height: 4rem;
      border-radius: 3rem;
      background-color: #f4f5f8;
      box-shadow: inset 0 0 8px rgba(180, 180, 180, 0.4);
      cursor: pointer;
    }

    .pump-switch input[type="checkbox"] {
      position: absolute;
      opacity: 0;
      top: -20px;
      pointer-events: none;
    }

    .pump-switch.loading {
      opacity: 0.5;
      pointer-events: none;
    }

    .pump-switch .toggle {
      position: absolute;
      display: flex;
      justify-content: center;
      align-items: center;
      width: 4rem;
      height: 4rem;
      border-radius: 50%;
      transition: width 250ms ease-out, left 250ms ease-out;
      left: 0;
      background: var(--pump-orange);
    }

    .pump-switch.loading .toggle {
      width: 4rem;
      background: var(--pump-disabled);
    }

    .pump-switch[data-state="on"] .toggle {
      left: calc(100% - 4rem);
      background: var(--pump-green);
    }

    .pump-switch[data-state="off"] .toggle {
      left: 0;
      background: var(--pump-orange);
    }

    .pump-switch[data-state="disconnected"] .toggle {
      left: calc(50% - 2rem);
      background: var(--pump-disabled);
    }

    .pump-status {
      width: 100%;
      font-family: Arial, sans-serif;
      font-size: 1.2rem;
      font-weight: bold;
      text-transform: uppercase;
      letter-spacing: 0.05em;
      text-align: center;
      color: #444;
    }

    .pump-status[data-state="on"] {
      color: #1d7f33;
    }

    .pump-status[data-state="off"] {
      color: #b35a00;
    }

    .pump-status[data-state="disconnected"],
    .pump-status[data-state="loading"] {
      color: #6e7587;
    }

    .pump-spinner {
      opacity: 0;
      width: 2rem;
      height: 2rem;
      border: 0.35rem solid white;
      border-bottom-color: transparent;
      border-radius: 50%;
      display: inline-block;
      box-sizing: border-box;
      animation: pump-rotate 1s linear infinite;
    }

    .pump-switch.loading .pump-spinner {
      opacity: 1;
    }

    @keyframes pump-rotate {
      0% {
        transform: rotate(0deg);
      }

      100% {
        transform: rotate(360deg);
      }
    }
  </style>
</head>
<body>
  <table border="0">
    <tr valign="top">
      <td bgcolor="#aaa" width="640">
        <img
          src="makegraph.php"
          alt="Temperature Graph"
          id="reloader"
          onload="setTimeout('document.getElementById(\'reloader\').src=\'makegraph.php?\'+new Date().getTime()', 5000)"
          onerror="setTimeout('document.getElementById(\'reloader\').src=\'makegraph.php?\'+new Date().getTime()', 5000)"
        >
      </td>

      <td bgcolor="#eee">
        <div class="pump-controls">
        <form action="turnon.php" id="turn-on-form">
          <input type="submit" style="height:100px;width:200px" value="Turn pump ON" />
        </form>

        <form action="turnoff.php" id="turn-off-form">
          <input type="submit" style="height:100px;width:200px" value="Turn pump OFF" />
        </form>

        <div class="pump-switch-row">
          <label class="pump-switch" for="pump-toggle" data-state="off">
            <input type="checkbox" id="pump-toggle" name="pump-toggle" />
            <div class="toggle">
              <div class="pump-spinner"></div>
            </div>
          </label>
          <div class="pump-status" id="pump-status" data-state="off">Off</div>
        </div>
        </div>
      </td>
    </tr>
  </table>

  <script>
    const pumpSwitch = document.querySelector('.pump-switch');
    const pumpToggle = document.getElementById('pump-toggle');
    const pumpStatus = document.getElementById('pump-status');

    function setPumpStatus(state) {
      const normalizedState = state.toLowerCase();
      const labels = {
        on: 'On',
        off: 'Off',
        disconnected: 'Disconnected',
        loading: 'Loading'
      };

      pumpStatus.dataset.state = normalizedState;
      pumpStatus.textContent = labels[normalizedState] || normalizedState;
    }

    function applyPumpState(state) {
      pumpSwitch.dataset.state = state;
      pumpToggle.checked = state === 'on';
      pumpToggle.indeterminate = state === 'disconnected';
      setPumpStatus(state);
    }

    function applyRemoteState(state) {
      if (state === 'loading') {
        pumpSwitch.classList.add('loading');
        setPumpStatus(state);
        return;
      }

      pumpSwitch.classList.remove('loading');
      applyPumpState(state.toLowerCase());
    }

    async function getJson(url, options = {}) {
      const response = await fetch(url, {
        ...options,
        headers: {
          'Content-Type': 'application/json',
          ...(options.headers || {})
        }
      });

      let payload = {};
      try {
        payload = await response.json();
      } catch (error) {
        payload = {};
      }

      if (!response.ok) {
        const message = payload.error || `${response.status} ${response.statusText}`;
        throw new Error(message);
      }

      return payload;
    }

    async function sendPower(value) {
      try {
        await getJson('/api/power', {
          method: 'POST',
          body: JSON.stringify({ value })
        });
      } catch (error) {
        pumpSwitch.classList.remove('loading');
        console.error('Error sending power state:', error);
      }
    }

    pumpSwitch.addEventListener('click', (event) => {
      event.preventDefault();
      if (pumpSwitch.classList.contains('loading')) {
        return;
      }

      const currentState = pumpSwitch.dataset.state || 'disconnected';
      if (currentState === 'on') {
        pumpSwitch.classList.add('loading');
        sendPower('OFF');
      } else if (currentState === 'off') {
        pumpSwitch.classList.add('loading');
        sendPower('ON');
      }
    });

    function waitLoop() {
      applyPumpState('disconnected');
      pumpSwitch.classList.add('loading');
      const eventSource = new EventSource('/api/events');

      eventSource.onmessage = (event) => {
        try {
          const payload = JSON.parse(event.data);
          if (payload.state) {
            applyRemoteState(payload.state);
          }
        } catch (error) {
          console.error('Error parsing event stream payload:', error);
        }
      };

      eventSource.onerror = () => {
        pumpSwitch.classList.add('loading');
      };

      return eventSource;
    }

    waitLoop();
  </script>

</body>
</html>
