document.addEventListener('DOMContentLoaded', () => {
    const socket = io();
    let botRunning = false;

    // UI Elements
    const statusDot = document.getElementById('status-dot');
    const statusText = document.getElementById('status-text');
    const mainControlBtn = document.getElementById('main-control-btn');
    const logDisplay = document.getElementById('log-display');

    // Metrics
    const mTrades = document.getElementById('m-trades');
    const mWinrate = document.getElementById('m-winrate');
    const mProfit = document.getElementById('m-profit');
    const mBalance = document.getElementById('m-balance');
    const mMarkets = document.getElementById('m-markets');

    // Tables
    const positionsTable = document.getElementById('positions-table');
    const resolvedTable = document.getElementById('resolved-table');
    const scanTable = document.getElementById('scan-table');
    const newsTable = document.getElementById('news-table');
    const devTable = document.getElementById('dev-table');
    const downloadLogsBtn = document.getElementById('download-logs-btn');

    // Tab Logic
    const tabs = document.querySelectorAll('.tab-btn');
    const panes = document.querySelectorAll('.tab-pane');

    tabs.forEach(tab => {
        tab.addEventListener('click', () => {
            tabs.forEach(t => t.classList.remove('active'));
            panes.forEach(p => p.style.display = 'none');

            tab.classList.add('active');
            document.getElementById(tab.dataset.tab).style.display = 'block';
        });
    });

    // Bot Control
    mainControlBtn.addEventListener('click', () => {
        const action = botRunning ? 'stop' : 'start';
        fetch('/api/control', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ action })
        })
        .then(res => res.json())
        .then(data => {
            updateBotUI(data.is_trading);
        });
    });

    function updateBotUI(isTrading) {
        botRunning = isTrading;
        if (isTrading) {
            statusDot.classList.add('active');
            statusText.innerText = 'Trading Active';
            mainControlBtn.innerText = 'Stop Trading';
            mainControlBtn.classList.remove('btn-success');
            mainControlBtn.classList.add('btn-danger');
        } else {
            statusDot.classList.remove('active');
            statusText.innerText = 'Scanning Only';
            mainControlBtn.innerText = 'Start Trading';
            mainControlBtn.classList.remove('btn-danger');
            mainControlBtn.classList.add('btn-success');
        }
    }

    let lastStatusJson = '';

    // Socket Updates
    socket.on('bot_status', (data) => {
        const statusJson = JSON.stringify(data);
        if (statusJson === lastStatusJson) return;
        lastStatusJson = statusJson;

        updateBotUI(data.is_trading);

        // Update Metrics
        mTrades.innerText = data.metrics.total_trades;
        mWinrate.innerText = data.metrics.win_rate + '%';
        mProfit.innerText = '$' + data.metrics.total_profit.toFixed(2);
        mBalance.innerText = '$' + data.metrics.balance.toLocaleString();
        if (mMarkets) mMarkets.innerText = data.total_scanned || 0;

        // Update settings inputs with current values if not already focused
        if (data.config) {
            const inputs = {
                's-mode': data.config.paper_mode.toString(),
                's-strategy': data.config.strategy,
                's-amount': data.config.trade_amount,
                's-edge': data.config.min_edge,
                's-interval': data.config.scan_interval,
                's-balance': data.config.paper_balance,
                's-max-trades': data.config.max_trades
            };
            for (const [id, val] of Object.entries(inputs)) {
                const el = document.getElementById(id);
                if (el && document.activeElement !== el) {
                    el.value = val;
                }
            }
        }

        // Update Logs
        logDisplay.innerHTML = data.logs.map(log => `<div class="log-entry">${log}</div>`).join('');
        logDisplay.scrollTop = logDisplay.scrollHeight;

        // Update Positions
        positionsTable.innerHTML = data.open_positions.map(p => `
            <tr>
                <td title="${p.market}">${p.market.substring(0, 50)}...</td>
                <td><span class="side-badge ${p.side.toLowerCase()}">${p.side}</span></td>
                <td>$${p.size}</td>
                <td>${p.price.toFixed(3)}</td>
                <td><span class="success">${p.signal_type || 'Bayesian'}</span></td>
            </tr>
        `).join('');

        // Update Resolved
        if (resolvedTable && data.resolved_positions) {
            resolvedTable.innerHTML = data.resolved_positions.map(p => {
                const profitClass = p.profit >= 0 ? 'success' : 'danger';
                return `
                    <tr>
                        <td title="${p.market}">${p.market.substring(0, 40)}...</td>
                        <td><span class="side-badge ${p.side.toLowerCase()}">${p.side}</span></td>
                        <td>$${p.size}</td>
                        <td class="${profitClass}">$${p.profit.toFixed(2)}</td>
                        <td>${p.resolved_at}</td>
                    </tr>
                `;
            }).reverse().join('');
        }

        // Update Scan
        const scanCountBadge = document.getElementById('scan-count-badge');
        if (scanCountBadge) {
            scanCountBadge.innerText = `${data.total_scanned || 0} Alpha Signals Found`;
        }
        if (data.scanned_markets && data.scanned_markets.length > 0) {
            const displayMarkets = data.scanned_markets.slice(0, 100);
            scanTable.innerHTML = displayMarkets.map(m => {
                const scoreColor = m.alpha_score > 70 ? 'success' : (m.alpha_score > 40 ? 'primary' : 'text-dim');
                return `
                    <tr>
                        <td title="${m.question}">${m.question.substring(0, 80)}...</td>
                        <td class="${scoreColor}" style="font-weight: bold;">${m.alpha_score.toFixed(1)}</td>
                        <td><span class="side-badge ${m.bias.toLowerCase()}">${m.bias}</span></td>
                        <td><span class="side-badge ${m.liquidity ? m.liquidity.toLowerCase() : 'high'}">${m.liquidity || 'High'}</span></td>
                        <td style="font-size: 0.8rem; color: var(--text-dim);">${m.reasoning}</td>
                    </tr>
                `;
            }).join('');
        } else {
            scanTable.innerHTML = '<tr><td colspan="4" style="text-align: center; padding: 40px; color: var(--text-dim);"><div class="loading-spinner"></div><br>Accumulating Bayesian evidence...</td></tr>';
        }

        // Update Alpha Feed (News)
        newsTable.innerHTML = data.news_events.map(e => `
            <tr>
                <td style="font-family: 'JetBrains Mono'; font-size: 0.8rem;">${e.trader}</td>
                <td><span class="side-badge yes" style="font-size:0.7rem;">${e.label}</span></td>
                <td>${e.activity}</td>
                <td class="success">${e.impact}</td>
                <td style="font-size: 0.75rem">${e.summary}</td>
            </tr>
        `).join('');

        // Update Dev Check Logs (Signal Attribution)
        if (devTable && data.dev_check_logs) {
            devTable.innerHTML = data.dev_check_logs.map(log => {
                const traders = log.top_traders.map(t => `<div style="margin-bottom: 5px; border-left: 2px solid var(--primary); padding-left: 5px;">${t.address.substring(0,8)}... (${t.label})</div>`).join('');
                return `
                    <tr>
                        <td style="font-size: 0.7rem; white-space: nowrap;">${log.timestamp}</td>
                        <td style="font-size: 0.8rem;"><span class="side-badge ${log.directional_bias.toLowerCase()}">${log.directional_bias}</span> ${log.question.substring(0, 40)}...</td>
                        <td style="font-size: 0.8rem; color: var(--primary-light);">${log.explanation || 'Aggregated Signal'}</td>
                        <td style="font-size: 0.7rem;">${traders || 'General Momentum'}</td>
                        <td style="font-size: 0.7rem; color: var(--text-dim);">
                            Strength: ${log.signal_strength.toFixed(4)} |
                            Count: ${log.top_traders.length}
                        </td>
                    </tr>
                `;
            }).reverse().join('');
        }
    });

    // Download Logs Logic
    if (downloadLogsBtn) {
        downloadLogsBtn.addEventListener('click', () => {
            const logs = lastStatusJson ? JSON.parse(lastStatusJson).dev_check_logs : [];
            if (logs.length === 0) {
                alert('No signals available to download.');
                return;
            }

            const logText = logs.map(l => {
                return `[${l.timestamp}] SIGNAL: ${l.directional_bias} | MARKET: ${l.question}\n` +
                       `STRENGTH: ${l.signal_strength.toFixed(4)}\n` +
                       `EXPLANATION: ${l.explanation}\n` +
                       `TRADERS: ${l.top_traders.map(t => `${t.address} (${t.label})`).join(', ')}\n` +
                       `--------------------------------------------------------------------------------\n`;
            }).join('\n');

            const blob = new Blob([logText], { type: 'text/plain' });
            const url = window.URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.href = url;
            a.download = `polymarket_alpha_signals_${new Date().toISOString().slice(0, 10)}.log`;
            document.body.appendChild(a);
            a.click();
            window.URL.revokeObjectURL(url);
            document.body.removeChild(a);
        });
    }

    // Settings Form
    const settingsForm = document.getElementById('settings-form');
    settingsForm.addEventListener('submit', (e) => {
        e.preventDefault();
        const config = {
            paper_mode: document.getElementById('s-mode').value === 'true',
            strategy: document.getElementById('s-strategy').value,
            trade_amount: parseFloat(document.getElementById('s-amount').value),
            min_edge: parseFloat(document.getElementById('s-edge').value),
            scan_interval: parseInt(document.getElementById('s-interval').value),
            paper_balance: parseFloat(document.getElementById('s-balance').value),
            max_trades: parseInt(document.getElementById('s-max-trades').value),
            private_key: document.getElementById('s-pk').value
        };

        fetch('/api/config', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(config)
        })
        .then(res => res.json())
        .then(data => {
            alert('Settings saved successfully!');
        });
    });

    // Load settings immediately on page load (don't wait for WebSocket)
    fetch('/api/config')
        .then(res => res.json())
        .then(cfg => {
            const inputs = {
                's-mode':       cfg.paper_mode.toString(),
                's-strategy':   cfg.strategy,
                's-amount':     cfg.trade_amount,
                's-edge':       cfg.min_edge,
                's-interval':   cfg.scan_interval,
                's-balance':    cfg.paper_balance,
                's-max-trades': cfg.max_trades,
            };
            for (const [id, val] of Object.entries(inputs)) {
                const el = document.getElementById(id);
                if (el) el.value = val;
            }
        })
        .catch(() => {}); // silent fail — WebSocket will populate later

    // Request initial update
    socket.emit('request_update');
});
