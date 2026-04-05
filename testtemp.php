<!DOCTYPE html>
<html>
<head>
  <meta http-equiv="content-type" content="text/html; charset=UTF-8">
  <meta name="robots" content="noindex, nofollow">
  <meta name="googlebot" content="noindex, nofollow">
  <!--meta http-equiv="refresh" content="5"-->

  <title>Temperature Update</title>
  <style>
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
      display: -webkit-flex;
      display: flex;
      -webkit-justify-content: center;
      justify-content: center;
      -webkit-align-items: center;
      align-items: center;
      width: 4rem;
      height: 4rem;
      border-radius: 50%;
      -webkit-transition: width 250ms ease-out, left 250ms ease-out;
      transition: width 250ms ease-out, left 250ms ease-out;
      left: 0;
      background: #ff9500;
    }

    .pump-switch.loading .toggle {
      width: 4rem;
      background: #9ba2b5;
    }

    .pump-switch[data-state="on"] .toggle {
      left: calc(100% - 4rem);
      background: #4bd964;
    }

    .pump-switch[data-state="off"] .toggle {
      left: 0;
      background: #ff9500;
    }

    .pump-switch[data-state="disconnected"] .toggle {
      left: calc(50% - 2rem);
      background: #9ba2b5;
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
      -webkit-animation: pump-rotate 1s linear infinite;
      animation: pump-rotate 1s linear infinite;
    }

    .pump-switch.loading .pump-spinner {
      opacity: 1;
    }

    @-webkit-keyframes pump-rotate {
      0%   { -webkit-transform: rotate(0deg);   transform: rotate(0deg); }
      100% { -webkit-transform: rotate(360deg); transform: rotate(360deg); }
    }
    @keyframes pump-rotate {
      0%   { -webkit-transform: rotate(0deg);   transform: rotate(0deg); }
      100% { -webkit-transform: rotate(360deg); transform: rotate(360deg); }
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
    var pumpSwitch = document.querySelector('.pump-switch');
    var pumpToggle = document.getElementById('pump-toggle');
    var pumpStatus = document.getElementById('pump-status');

    function setPumpStatus(state) {
      var normalizedState = state.toLowerCase();
      var labels = { on: 'On', off: 'Off', disconnected: 'Disconnected', loading: 'Loading' };
      pumpStatus.dataset.state = normalizedState;
      pumpStatus.textContent = labels[normalizedState] || normalizedState;
    }

    function applyPumpState(state) {
      pumpSwitch.dataset.state = state;
      pumpToggle.checked = (state === 'on');
      pumpToggle.indeterminate = (state === 'disconnected');
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

    // Returns a Promise resolving to the parsed JSON payload.
    // method: 'GET' or 'POST', bodyStr: optional JSON string
    function getJson(url, method, bodyStr) {
      var opts = { method: method || 'GET', headers: { 'Content-Type': 'application/json' } };
      if (bodyStr !== undefined) { opts.body = bodyStr; }
      return fetch(url, opts).then(function(response) {
        var status = response.status;
        var ok = response.ok;
        var statusText = response.statusText;
        return response.json().then(
          function(payload) {
            if (!ok) { throw new Error(payload.error || (status + ' ' + statusText)); }
            return payload;
          },
          function() {
            if (!ok) { throw new Error(status + ' ' + statusText); }
            return {};
          }
        );
      });
    }

    function sendPower(value) {
      getJson('/api/power', 'POST', JSON.stringify({ value: value })).catch(function(error) {
        pumpSwitch.classList.remove('loading');
      });
    }

    pumpSwitch.addEventListener('click', function(event) {
      event.preventDefault();
      if (pumpSwitch.classList.contains('loading')) { return; }
      var currentState = pumpSwitch.dataset.state || 'disconnected';
      if (currentState === 'on') {
        pumpSwitch.classList.add('loading');
        sendPower('OFF');
      } else if (currentState === 'off') {
        pumpSwitch.classList.add('loading');
        sendPower('ON');
      }
    });

    function poll(currentState) {
      var url = '/api/wait_for_change?interval=30' + (currentState ? ('&state=' + encodeURIComponent(currentState)) : '');
      getJson(url).then(function(payload) {
        if (payload.state) {
          applyRemoteState(payload.state);
          poll(payload.state);
        } else {
          poll(currentState);
        }
      }).catch(function(error) {
        pumpSwitch.classList.add('loading');
        setTimeout(function() { poll(currentState); }, 5000);
      });
    }

    applyPumpState('disconnected');
    pumpSwitch.classList.add('loading');
    poll('');
  </script>

</body>
</html>
