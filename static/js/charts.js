const CLASS_COLORS = {
    'Eau': '#2196F3',
    'Sol nu': '#795548',
    'Urbain': '#9E9E9E',
    'Cultures (canne, etc.)': '#FFC107',
    'Cocotiers probables': '#4CAF50',
    'Foret dense': '#1B5E20',
    'Autre vegetation': '#8BC34A',
};

const EXCLUDE_CLASSES = ['Eau', 'Sol nu'];

function createDoughnutChart(canvasId, data) {
    const filtered = Object.entries(data).filter(([k]) => !EXCLUDE_CLASSES.includes(k));
    const labels = filtered.map(([k]) => k);
    const values = filtered.map(([, v]) => v.hectares);
    const colors = labels.map(l => CLASS_COLORS[l] || '#ccc');

    new Chart(document.getElementById(canvasId), {
        type: 'doughnut',
        data: {
            labels: labels,
            datasets: [{
                data: values,
                backgroundColor: colors,
                borderWidth: 2,
                borderColor: '#fff',
                hoverOffset: 8
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    position: 'right',
                    labels: { padding: 12, usePointStyle: true, pointStyle: 'rectRounded', font: { size: 12 } }
                },
                tooltip: {
                    callbacks: {
                        label: function(ctx) {
                            const total = ctx.dataset.data.reduce((a, b) => a + b, 0);
                            const pct = ((ctx.parsed / total) * 100).toFixed(1);
                            return ` ${ctx.label}: ${ctx.parsed.toLocaleString('fr-FR')} ha (${pct}%)`;
                        }
                    }
                }
            }
        }
    });
}

function createBarChart(canvasId, data) {
    let entries = Object.entries(data);
    // Exclure eau et ocean (inclus dans la bbox mais hors analyse)
    entries = entries.filter(([k]) => !EXCLUDE_CLASSES.includes(k));
    // Trier par surface decroissante
    entries.sort((a, b) => b[1].hectares - a[1].hectares);

    const labels = entries.map(([k]) => k);
    const values = entries.map(([, v]) => v.hectares);
    const colors = labels.map(l => CLASS_COLORS[l] || '#ccc');

    new Chart(document.getElementById(canvasId), {
        type: 'bar',
        data: {
            labels: labels,
            datasets: [{
                data: values,
                backgroundColor: colors,
                borderRadius: 4,
                borderSkipped: false,
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            indexAxis: 'y',
            plugins: {
                legend: { display: false },
                tooltip: {
                    callbacks: {
                        label: function(ctx) {
                            return ` ${ctx.parsed.x.toLocaleString('fr-FR')} ha (${(ctx.parsed.x / 100).toLocaleString('fr-FR', {maximumFractionDigits: 1})} km²)`;
                        }
                    }
                }
            },
            scales: {
                x: {
                    ticks: {
                        callback: function(v) {
                            if (v >= 1000) return (v / 1000).toFixed(0) + 'k';
                            return v;
                        },
                        font: { size: 11 }
                    },
                    title: { display: true, text: 'Hectares', font: { size: 12 } }
                },
                y: {
                    ticks: { font: { size: 12 } }
                }
            }
        }
    });
}
