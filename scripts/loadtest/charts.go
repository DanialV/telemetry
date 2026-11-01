package main

import (
	"bytes"
	"fmt"
	"html"
	"math"
	"os"
	"path/filepath"
)

type seriesLine struct {
	Label  string
	Color  string
	Dashed bool
	Values []float64
}

func generateAllCharts(outDir string, results []stepResult) error {
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return err
	}

	xLabels := make([]string, len(results))
	targetRates := make([]float64, len(results))
	ingestRates := make([]float64, len(results))
	persistRates := make([]float64, len(results))

	lat5mP95 := make([]float64, len(results))
	lat24hP95 := make([]float64, len(results))
	lat1yP50 := make([]float64, len(results))
	lat1yP95 := make([]float64, len(results))
	sloLine := make([]float64, len(results))

	cpuIngest := make([]float64, len(results))
	cpuProc := make([]float64, len(results))
	cpuNats := make([]float64, len(results))
	cpuCH := make([]float64, len(results))
	memCH := make([]float64, len(results))
	memTotal := make([]float64, len(results))

	for i, r := range results {
		xLabels[i] = fmt.Sprintf("%dk", r.Devices/1000)
		if r.Devices < 1000 {
			xLabels[i] = fmt.Sprintf("%d", r.Devices)
		}
		targetRates[i] = float64(r.Devices)
		ingestRates[i] = r.IngestRatePerSec
		persistRates[i] = r.PersistRatePerSec

		lat5mP95[i] = r.Queries["5m"].P95Ms
		lat24hP95[i] = r.Queries["24h"].P95Ms
		lat1yP50[i] = r.Queries["1y"].P50Ms
		lat1yP95[i] = r.Queries["1y"].P95Ms
		sloLine[i] = 1000.0

		cpuIngest[i] = r.Resources["ingestor"].CPUPercent
		cpuProc[i] = r.Resources["processor"].CPUPercent
		cpuNats[i] = r.Resources["nats"].CPUPercent
		cpuCH[i] = r.Resources["clickhouse"].CPUPercent
		memCH[i] = r.Resources["clickhouse"].MemMiB
		var sumMem float64
		for _, u := range r.Resources {
			sumMem += u.MemMiB
		}
		memTotal[i] = sumMem
	}

	writeSVG := renderLineChartSVG(
		"Write Throughput vs. Simulated Device Count",
		"Simulated Devices (1 reading/sec each)",
		"Samples / sec",
		xLabels,
		[]seriesLine{
			{Label: "Target Rate", Color: "#8b949e", Dashed: true, Values: targetRates},
			{Label: "Ingested (JetStream)", Color: "#2f81f7", Values: ingestRates},
			{Label: "Persisted (ClickHouse)", Color: "#3fb950", Values: persistRates},
		},
	)
	if err := os.WriteFile(filepath.Join(outDir, "write_rate.svg"), writeSVG, 0o644); err != nil {
		return err
	}

	latencySVG := renderLineChartSVG(
		"Chart Query Response Latency Under Write Load (SLO < 1000 ms)",
		"Concurrent Simulated Devices",
		"Response Latency (ms)",
		xLabels,
		[]seriesLine{
			{Label: "1y Rollup p50", Color: "#3fb950", Values: lat1yP50},
			{Label: "1y Rollup p95", Color: "#2f81f7", Values: lat1yP95},
			{Label: "24h Rollup p95", Color: "#a371f7", Values: lat24hP95},
			{Label: "5m Raw p95", Color: "#d29922", Values: lat5mP95},
			{Label: "1000ms SLO Limit", Color: "#f85149", Dashed: true, Values: sloLine},
		},
	)
	if err := os.WriteFile(filepath.Join(outDir, "query_latency.svg"), latencySVG, 0o644); err != nil {
		return err
	}

	resourceSVG := renderDualPanelResourceSVG(xLabels,
		[]seriesLine{
			{Label: "Ingestor CPU%", Color: "#2f81f7", Values: cpuIngest},
			{Label: "Processor CPU%", Color: "#a371f7", Values: cpuProc},
			{Label: "NATS CPU%", Color: "#d29922", Values: cpuNats},
			{Label: "ClickHouse CPU%", Color: "#f85149", Values: cpuCH},
		},
		[]seriesLine{
			{Label: "ClickHouse Mem (MiB)", Color: "#f85149", Values: memCH},
			{Label: "Total Stack Mem (MiB)", Color: "#39c5cf", Values: memTotal},
		},
	)
	return os.WriteFile(filepath.Join(outDir, "resource_usage.svg"), resourceSVG, 0o644)
}

func renderLineChartSVG(title, xAxisLabel, yAxisLabel string, xLabels []string, lines []seriesLine) []byte {
	const W, H = 880.0, 460.0
	const padL, padR, padT, padB = 82.0, 36.0, 68.0, 68.0
	plotW := W - padL - padR
	plotH := H - padT - padB

	maxY := 10.0
	for _, l := range lines {
		if l.Dashed && l.Label == "1000ms SLO Limit" {
			continue
		}
		for _, v := range l.Values {
			if v > maxY {
				maxY = v
			}
		}
	}
	for _, l := range lines {
		if l.Label == "1000ms SLO Limit" && maxY > 250 {
			maxY = max(maxY, 1050)
		}
	}
	maxY = niceCeil(maxY * 1.12)

	var b bytes.Buffer
	fmt.Fprintf(&b, `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 %.0f %.0f" width="%.0f" height="%.0f">`, W, H, W, H)
	b.WriteString(`<rect width="100%" height="100%" fill="#0d1117" rx="10"/>`)
	fmt.Fprintf(&b, `<text x="%.0f" y="32" fill="#e6edf3" font-family="-apple-system,sans-serif" font-size="16" font-weight="700">%s</text>`, padL, html.EscapeString(title))

	legendX := padL
	for _, l := range lines {
		if l.Label == "1000ms SLO Limit" && maxY < 250 {
			continue
		}
		fmt.Fprintf(&b, `<line x1="%.0f" y1="50" x2="%.0f" y2="50" stroke="%s" stroke-width="2.5"%s/>`,
			legendX, legendX+18, l.Color, dashAttr(l.Dashed))
		fmt.Fprintf(&b, `<text x="%.0f" y="54" fill="#c9d1d9" font-family="-apple-system,sans-serif" font-size="11.5">%s</text>`,
			legendX+23, html.EscapeString(l.Label))
		legendX += float64(len(l.Label))*6.8 + 42
	}

	const yTicks = 5
	for i := 0; i <= yTicks; i++ {
		frac := float64(i) / yTicks
		y := padT + plotH*(1-frac)
		val := maxY * frac
		fmt.Fprintf(&b, `<line x1="%.1f" y1="%.1f" x2="%.1f" y2="%.1f" stroke="#21262d" stroke-width="1"/>`, padL, y, padL+plotW, y)
		fmt.Fprintf(&b, `<text x="%.1f" y="%.1f" text-anchor="end" fill="#8b949e" font-family="-apple-system,sans-serif" font-size="11">%s</text>`,
			padL-10, y+4, formatMetricVal(val))
	}

	n := len(xLabels)
	xPos := func(idx int) float64 {
		if n <= 1 {
			return padL + plotW/2
		}
		return padL + plotW*float64(idx)/float64(n-1)
	}
	for i, lbl := range xLabels {
		x := xPos(i)
		fmt.Fprintf(&b, `<line x1="%.1f" y1="%.1f" x2="%.1f" y2="%.1f" stroke="#21262d" stroke-width="1"/>`, x, padT, x, padT+plotH)
		fmt.Fprintf(&b, `<text x="%.1f" y="%.1f" text-anchor="middle" fill="#8b949e" font-family="-apple-system,sans-serif" font-size="11">%s</text>`,
			x, padT+plotH+20, lbl)
	}

	for _, l := range lines {
		if len(l.Values) == 0 || (l.Label == "1000ms SLO Limit" && maxY < 250) {
			continue
		}
		b.WriteString(`<path fill="none" stroke="` + l.Color + `" stroke-width="2.4"` + dashAttr(l.Dashed) + ` d="`)
		for i, v := range l.Values {
			clamped := min(v, maxY)
			x := xPos(i)
			y := padT + plotH*(1-clamped/maxY)
			cmd := "L"
			if i == 0 {
				cmd = "M"
			}
			fmt.Fprintf(&b, "%s%.1f,%.1f ", cmd, x, y)
		}
		b.WriteString(`"/>`)
		if !l.Dashed {
			for i, v := range l.Values {
				clamped := min(v, maxY)
				x := xPos(i)
				y := padT + plotH*(1-clamped/maxY)
				fmt.Fprintf(&b, `<circle cx="%.1f" cy="%.1f" r="3.5" fill="%s"/>`, x, y, l.Color)
			}
		}
	}

	fmt.Fprintf(&b, `<text x="%.1f" y="%.1f" text-anchor="middle" fill="#8b949e" font-family="-apple-system,sans-serif" font-size="12">%s</text>`,
		padL+plotW/2, H-16, html.EscapeString(xAxisLabel))
	fmt.Fprintf(&b, `<text transform="translate(20, %.1f) rotate(-90)" text-anchor="middle" fill="#8b949e" font-family="-apple-system,sans-serif" font-size="12">%s</text>`,
		padT+plotH/2, html.EscapeString(yAxisLabel))
	b.WriteString(`</svg>`)
	return b.Bytes()
}

func renderDualPanelResourceSVG(xLabels []string, cpuLines, memLines []seriesLine) []byte {
	top := renderLineChartSVG("Container CPU Utilization Under Load (100% = 1 vCPU)", "Simulated Devices", "CPU %", xLabels, cpuLines)
	bot := renderLineChartSVG("Container Memory Footprint Under Load", "Simulated Devices", "Memory (MiB)", xLabels, memLines)

	stripOuter := func(svg []byte) string {
		s := string(svg)
		start := bytes.IndexByte(svg, '>') + 1
		end := len(s) - len("</svg>")
		return s[start:end]
	}

	var b bytes.Buffer
	b.WriteString(`<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 880 940" width="880" height="940">`)
	b.WriteString(`<rect width="100%" height="100%" fill="#0d1117" rx="10"/>`)
	b.WriteString(`<g transform="translate(0,0)">` + stripOuter(top) + `</g>`)
	b.WriteString(`<g transform="translate(0,480)">` + stripOuter(bot) + `</g>`)
	b.WriteString(`</svg>`)
	return b.Bytes()
}

func dashAttr(dashed bool) string {
	if dashed {
		return ` stroke-dasharray="6,4"`
	}
	return ""
}

func niceCeil(v float64) float64 {
	if v <= 0 {
		return 10
	}
	exp := math.Pow(10, math.Floor(math.Log10(v)))
	f := v / exp
	switch {
	case f <= 1:
		return 1 * exp
	case f <= 2:
		return 2 * exp
	case f <= 5:
		return 5 * exp
	default:
		return 10 * exp
	}
}

func formatMetricVal(v float64) string {
	if v >= 1000 {
		return fmt.Sprintf("%.1fk", v/1000)
	}
	if v >= 10 {
		return fmt.Sprintf("%.0f", v)
	}
	return fmt.Sprintf("%.1f", v)
}
