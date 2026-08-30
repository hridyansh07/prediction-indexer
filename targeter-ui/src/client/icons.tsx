import kalshi from './assets/kalshi.svg';
import limitless from './assets/limitless.svg';
import polymarket from './assets/polymarket.svg';

const venueAssets: Record<string, { src: string; name: string }> = {
  kalshi: { src: kalshi, name: 'Kalshi' },
  polymarket: { src: polymarket, name: 'Polymarket' },
  limitless: { src: limitless, name: 'Limitless' },
};

const games: Record<string, { name: string; src?: string; glyph?: string }> = {
  counter_strike_2: {
    name: 'Counter-Strike 2',
    src: 'https://cdn.cloudflare.steamstatic.com/apps/csgo/images/csgo_react/cs2/logo_cs2_header.svg',
  },
  league_of_legends: {
    name: 'League of Legends',
    src: 'https://upload.wikimedia.org/wikipedia/commons/d/d8/League_of_Legends_2019_vector.svg',
  },
  basketball: { name: 'Basketball', glyph: '●' },
  soccer: { name: 'Soccer', glyph: '⬡' },
};

export const gameName = (game: string | null, sport = '') =>
  games[game ?? '']?.name ??
  (game || sport || 'Event')
    .replaceAll('_', ' ')
    .replace(/\b\w/g, (character) => character.toUpperCase());

export function EventIcon({
  game,
  sport,
  labelled = false,
}: {
  game: string | null;
  sport?: string;
  labelled?: boolean;
}) {
  const key = game ?? sport ?? '';
  const asset = games[key];
  const name = gameName(game, sport);
  return (
    <span className={`event-icon ${labelled ? 'labelled' : ''}`} title={name}>
      {asset?.src ? (
        <img src={asset.src} alt="" />
      ) : (
        <span className={`sport-glyph ${key}`}>{asset?.glyph ?? '◆'}</span>
      )}
      {labelled && <span>{name}</span>}
    </span>
  );
}

export function VenueStack({ venues }: { venues: string[] }) {
  const unique = [...new Set(venues)];
  const visible = unique.slice(0, 4);
  const overflow = Math.max(0, unique.length - visible.length);
  return (
    <span
      className="venue-stack"
      aria-label={visible.map(venueName).join(', ')}
    >
      {visible.map((venue) => {
        const asset = venueAssets[venue];
        return (
          <span
            className={`venue-icon ${venue}`}
            key={venue}
            title={venueName(venue)}
          >
            {asset ? (
              <img src={asset.src} alt="" />
            ) : (
              venue.slice(0, 1).toUpperCase()
            )}
            <span className="venue-label">{venueName(venue)}</span>
          </span>
        );
      })}
      {!!overflow && <span className="venue-overflow">+{overflow}</span>}
    </span>
  );
}

export const venueName = (venue: string) =>
  venueAssets[venue]?.name ??
  venue
    .replaceAll('_', ' ')
    .replace(/\b\w/g, (character) => character.toUpperCase());

export function Chevron() {
  return (
    <span className="chevron" aria-hidden="true">
      ›
    </span>
  );
}

export function SearchIcon() {
  return <span aria-hidden="true">⌕</span>;
}
