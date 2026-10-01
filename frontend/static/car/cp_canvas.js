/**
 * Shared HiDPI canvas setup for the car page's two line charts (TCO operating cost
 * and depreciation). They used to carry identical copies (setupTcoCostCanvas /
 * setupDepreciationCanvas) that differed only in the frame selector.
 *
 * CP.setupHiDpiCanvas(canvas, frameSelector) sizes the canvas to its frame
 * (max 408 css px wide, 0.58 aspect), scales the backing store by
 * devicePixelRatio and clears it. Returns {ctx, cssWidth, cssHeight}, or null
 * when the frame is still too narrow to draw (hidden tab) or there is no 2d
 * context.
 *
 * The axis helpers draw the frame both charts share, in the order they always
 * drew it: CP.drawChartGrid (4 horizontal rules), CP.drawChartYTicks (value
 * labels on those rules, top = max), then the caller's series, then
 * CP.drawChartXLabels (year label + mileage sub-label under each step).
 * Loaded before car_tco.js and car_page.js (see car.html).
 */
window.CP = window.CP || {};
(function (CP) {
    "use strict";

    function setupHiDpiCanvas(canvas, frameSelector) {
        const frame = canvas.closest(frameSelector);
        const frameInnerWidth = frame
            ? Math.max(frame.clientWidth - 32, 240)
            : Math.max(canvas.clientWidth || 320, 280);
        if (frame && frameInnerWidth <= 240) {
            return null;
        }
        const cssWidth = Math.floor(Math.min(frameInnerWidth, 408));
        const cssHeight = Math.floor(cssWidth * 0.58);
        const dpr = Math.max(window.devicePixelRatio || 1, 1);

        canvas.width = Math.floor(cssWidth * dpr);
        canvas.height = Math.floor(cssHeight * dpr);
        canvas.style.width = cssWidth + "px";
        canvas.style.height = cssHeight + "px";

        const ctx = canvas.getContext("2d");
        if (!ctx) return null;
        ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
        ctx.imageSmoothingEnabled = true;
        ctx.imageSmoothingQuality = "high";
        ctx.clearRect(0, 0, cssWidth, cssHeight);
        return { ctx: ctx, cssWidth: cssWidth, cssHeight: cssHeight };
    }

    const CHART_PAD = { top: 16, right: 12, bottom: 44, left: 44 };
    const CHART_GRID_COLOR = "#e2e8f0";
    const CHART_LABEL_COLOR = "#64748b";
    const CHART_MILEAGE_COLOR = "#94a3b8";
    const CHART_LABEL_FONT = "600 11px system-ui, -apple-system, BlinkMacSystemFont, sans-serif";
    const CHART_SUBLABEL_FONT = "500 10px system-ui, -apple-system, BlinkMacSystemFont, sans-serif";

    /**
     * Geometry for a chart of (steps + 1) points spanning [minVal, minVal + valSpan]
     * on a surface from setupHiDpiCanvas.
     */
    function chartPlotArea(surface, minVal, valSpan, steps) {
        const pad = CHART_PAD;
        const chartW = surface.cssWidth - pad.left - pad.right;
        const chartH = surface.cssHeight - pad.top - pad.bottom;
        return {
            ctx: surface.ctx,
            pad: pad,
            chartW: chartW,
            chartH: chartH,
            chartBottom: pad.top + chartH,
            minVal: minVal,
            valSpan: valSpan,
            steps: steps,
            xAt: function (index) {
                return pad.left + (chartW * index) / steps;
            },
            yAt: function (val) {
                return pad.top + chartH * (1 - (val - minVal) / valSpan);
            },
        };
    }

    function drawChartGrid(plot) {
        const ctx = plot.ctx;
        const pad = plot.pad;
        ctx.strokeStyle = CHART_GRID_COLOR;
        ctx.lineWidth = 1;
        for (let g = 0; g <= 3; g += 1) {
            const gy = pad.top + (plot.chartH * g) / 3;
            ctx.beginPath();
            ctx.moveTo(pad.left, gy);
            ctx.lineTo(pad.left + plot.chartW, gy);
            ctx.stroke();
        }
    }

    function drawChartYTicks(plot, formatTick) {
        const ctx = plot.ctx;
        const pad = plot.pad;
        ctx.fillStyle = CHART_LABEL_COLOR;
        ctx.font = CHART_LABEL_FONT;
        ctx.textAlign = "right";
        ctx.textBaseline = "middle";
        for (let g = 0; g <= 3; g += 1) {
            const tickVal = plot.minVal + (plot.valSpan * (3 - g)) / 3;
            const gy = pad.top + (plot.chartH * g) / 3;
            ctx.fillText(formatTick(tickVal), pad.left - 6, gy);
        }
    }

    /** labels[i] under step i, sublabels[i] (mileage) under that. */
    function drawChartXLabels(plot, labels, sublabels) {
        const ctx = plot.ctx;
        ctx.textAlign = "center";
        ctx.textBaseline = "top";
        for (let i = 0; i <= plot.steps; i += 1) {
            const x = plot.xAt(i);
            ctx.fillStyle = CHART_LABEL_COLOR;
            ctx.font = CHART_LABEL_FONT;
            ctx.fillText(labels[i], x, plot.chartBottom + 6);
            ctx.fillStyle = CHART_MILEAGE_COLOR;
            ctx.font = CHART_SUBLABEL_FONT;
            ctx.fillText(sublabels[i], x, plot.chartBottom + 20);
        }
    }

    /** The year labels under both five-year charts. */
    CP.CHART_YEAR_LABELS = Object.freeze(["Now", "Yr 1", "Yr 2", "Yr 3", "Yr 4", "Yr 5"]);
    CP.setupHiDpiCanvas = setupHiDpiCanvas;
    CP.chartPlotArea = chartPlotArea;
    CP.drawChartGrid = drawChartGrid;
    CP.drawChartYTicks = drawChartYTicks;
    CP.drawChartXLabels = drawChartXLabels;
})(window.CP);
