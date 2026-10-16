import { useEffect, useRef } from 'react';
import uPlot from 'uplot';
import 'uplot/dist/uPlot.min.css';
import { formatOffset, type ChartData } from './research-model';

export interface BookStyle {
  color: string;
  dash: number[];
  width: number;
}

export interface PlotLayout {
  left: number;
  width: number;
}

interface Props {
  data: ChartData;
  view: [number, number];
  yRange: [number, number];
  styles: BookStyle[];
  hidden: Set<number>;
  highlight: Set<number> | null;
  midOnly: boolean;
  cursor: number | null;
  height?: number;
  onView: (view: [number, number]) => void;
  onCursor: (t: number) => void;
  onReset: () => void;
  onLayout: (layout: PlotLayout) => void;
  onHover?: (t: number | null) => void;
  tier?: string;
}

// Tick steps in seconds that read as clock times, from 1 ms to 6 h.
const TIME_INCREMENTS = [
  0.001, 0.002, 0.005, 0.01, 0.02, 0.05, 0.1, 0.2, 0.5, 1, 2, 5, 10, 15, 30, 60,
  120, 300, 600, 900, 1800, 3600, 7200, 10800, 21600,
];

function alpha(color: string, a: number): string {
  const n = parseInt(color.slice(1), 16);
  return `rgba(${(n >> 16) & 255}, ${(n >> 8) & 255}, ${n & 255}, ${a})`;
}

/** One dominant price chart: x is seconds since capture start, y is cents per $1. */
export function ResearchChart(props: Props) {
  const host = useRef<HTMLDivElement>(null);
  const plot = useRef<uPlot | null>(null);
  const latest = useRef(props);
  latest.current = props;

  useEffect(() => {
    const element = host.current;
    if (!element) return;
    const { data, styles, hidden, highlight, midOnly, height = 380 } = props;
    const series: uPlot.Series[] = [{ label: 'time' }];
    const columns: (number | null)[][] = [];
    const bands: uPlot.Band[] = [];
    const paths = data.stepped ? uPlot.paths.stepped!({ align: 1 }) : undefined;
    data.books.forEach((book, index) => {
      const style = styles[index];
      const dim = highlight !== null && !highlight.has(index);
      const show = !hidden.has(index);
      const base = {
        show,
        spanGaps: false,
        paths,
        points: { show: false },
      } as const;
      const bidIndex = series.length;
      // Thin bid and ask edges, so a one-sided book (no mid) is still drawn.
      const edge = midOnly
        ? { stroke: 'transparent', width: 0 }
        : {
            stroke: alpha(style.color, dim ? 0.15 : 0.55),
            width: 1,
            dash: style.dash,
          };
      series.push({ ...base, label: `bid ${index}`, ...edge });
      series.push({ ...base, label: `ask ${index}`, ...edge });
      series.push({
        ...base,
        label: `mid ${index}`,
        stroke: dim ? alpha(style.color, 0.25) : style.color,
        width: highlight?.has(index) ? style.width + 1 : style.width,
        dash: style.dash,
      });
      columns.push(book.bid, book.ask, book.mid);
      if (!midOnly && show) {
        bands.push({
          series: [bidIndex + 1, bidIndex],
          fill: alpha(style.color, dim ? 0.04 : 0.12),
        });
      }
    });
    const options: uPlot.Options = {
      width: element.clientWidth || 800,
      height,
      legend: { show: false },
      series,
      bands,
      scales: {
        x: { time: false, auto: false, range: () => latest.current.view },
        y: { auto: false, range: () => latest.current.yRange },
      },
      axes: [
        {
          stroke: '#9595a3',
          grid: { stroke: '#2d2d37', width: 1 },
          ticks: { stroke: '#2d2d37' },
          values: (_u, ticks) => ticks.map((t) => formatOffset(t)),
          incrs: TIME_INCREMENTS,
          space: 90,
        },
        {
          stroke: '#9595a3',
          grid: { stroke: '#2d2d37', width: 1 },
          ticks: { stroke: '#2d2d37' },
          label: '¢ per $1',
          size: 52,
        },
      ],
      cursor: {
        drag: { x: true, y: false, setScale: false },
        points: { show: false },
        bind: { dblclick: () => () => null },
      },
      hooks: {
        init: [
          (u) => {
            u.over.addEventListener(
              'wheel',
              (event) => {
                event.preventDefault();
                const [v0, v1] = latest.current.view;
                const at = u.posToVal(event.offsetX, 'x');
                const factor = event.deltaY > 0 ? 1.25 : 0.8;
                latest.current.onView([
                  at - (at - v0) * factor,
                  at + (v1 - at) * factor,
                ]);
              },
              { passive: false },
            );
            u.over.addEventListener('dblclick', () => latest.current.onReset());
            u.over.addEventListener('mouseleave', () =>
              latest.current.onHover?.(null),
            );
          },
        ],
        setSelect: [
          (u) => {
            const { left, width } = u.select;
            if (width > 3) {
              latest.current.onView([
                u.posToVal(left, 'x'),
                u.posToVal(left + width, 'x'),
              ]);
            } else {
              latest.current.onCursor(u.posToVal(left, 'x'));
            }
            u.setSelect({ left: 0, top: 0, width: 0, height: 0 }, false);
          },
        ],
        setCursor: [
          (u) => {
            const left = u.cursor.left;
            latest.current.onHover?.(
              left === undefined || left < 0 ? null : u.posToVal(left, 'x'),
            );
          },
        ],
        draw: [
          (u) => {
            const t = latest.current.cursor;
            if (t === null) return;
            const x = Math.round(u.valToPos(t, 'x', true));
            if (x < u.bbox.left || x > u.bbox.left + u.bbox.width) return;
            const ctx = u.ctx;
            ctx.save();
            ctx.strokeStyle = '#e7e7ee';
            ctx.lineWidth = 1;
            ctx.beginPath();
            ctx.moveTo(x + 0.5, u.bbox.top);
            ctx.lineTo(x + 0.5, u.bbox.top + u.bbox.height);
            ctx.stroke();
            ctx.restore();
          },
        ],
        ready: [
          (u) => {
            const ratio = window.devicePixelRatio || 1;
            latest.current.onLayout({
              left: u.bbox.left / ratio,
              width: u.bbox.width / ratio,
            });
          },
        ],
      },
    };
    const instance = new uPlot(
      options,
      [data.x, ...columns] as uPlot.AlignedData,
      element,
    );
    plot.current = instance;
    const observer = new ResizeObserver(() => {
      instance.setSize({ width: element.clientWidth, height });
      const ratio = window.devicePixelRatio || 1;
      latest.current.onLayout({
        left: instance.bbox.left / ratio,
        width: instance.bbox.width / ratio,
      });
    });
    observer.observe(element);
    return () => {
      observer.disconnect();
      instance.destroy();
      plot.current = null;
    };
    // Rebuilt when the data or the drawing style changes; the view, y range and
    // cursor update the existing plot.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [props.data, props.styles, props.hidden, props.highlight, props.midOnly]);

  useEffect(() => {
    const instance = plot.current;
    if (!instance) return;
    instance.batch(() => {
      instance.setScale('x', { min: props.view[0], max: props.view[1] });
      instance.setScale('y', { min: props.yRange[0], max: props.yRange[1] });
    });
  }, [props.view, props.yRange]);

  useEffect(() => {
    plot.current?.redraw(false);
  }, [props.cursor]);

  return (
    <div
      className="research-chart"
      ref={host}
      data-tier={props.tier}
      data-stepped={String(props.data.stepped)}
      data-points={props.data.x.length}
    />
  );
}
