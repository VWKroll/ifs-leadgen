"use client";

import { AppSection } from "@/types/app-shell";

type NavItem = {
  section: AppSection;
  label: string;
  icon: React.ReactNode;
  badge?: number | string;
};

type Props = {
  activeSection: AppSection;
  onSectionChange: (section: AppSection) => void;
  triggerCount: number;
  claimedCount: number;
  activeRunCount: number;
  countryCount: number;
  onRefresh: () => void;
  userName?: string;
};

/* ── inline SVG icons ── */

function IconRadar() {
  return (
    <svg width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round">
      <circle cx="12" cy="12" r="10" />
      <path d="M12 12l4.2-4.2" />
      <path d="M12 12a2 2 0 1 0 0-4 2 2 0 0 0 0 4Z" />
      <path d="M17 12a5 5 0 1 0-5 5" />
    </svg>
  );
}

function IconGlobe() {
  return (
    <svg width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round">
      <circle cx="12" cy="12" r="10" />
      <path d="M2 12h20" />
      <path d="M12 2a15.3 15.3 0 0 1 4 10 15.3 15.3 0 0 1-4 10 15.3 15.3 0 0 1-4-10A15.3 15.3 0 0 1 12 2Z" />
    </svg>
  );
}

function IconChart() {
  return (
    <svg width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round">
      <path d="M3 3v18h18" />
      <path d="M7 16l4-8 4 4 5-6" />
    </svg>
  );
}

function IconBeaker() {
  return (
    <svg width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round">
      <path d="M9 2v6l-4 9a2 2 0 0 0 1.8 2.9h10.4A2 2 0 0 0 19 17l-4-9V2" />
      <path d="M8 2h8" />
      <path d="M7 16h10" />
    </svg>
  );
}

function IconUser() {
  return (
    <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round">
      <circle cx="12" cy="8" r="4" />
      <path d="M5.5 21a7.5 7.5 0 0 1 13 0" />
    </svg>
  );
}

function IconRefresh() {
  return (
    <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <path d="M21 12a9 9 0 1 1-6.2-8.6" />
      <path d="M21 3v6h-6" />
    </svg>
  );
}

export function NavRail({
  activeSection,
  onSectionChange,
  triggerCount,
  claimedCount,
  activeRunCount,
  countryCount,
  onRefresh,
  userName,
}: Props) {
  const items: NavItem[] = [
    {
      section: "event_intelligence",
      label: "Event Intelligence",
      icon: <IconRadar />,
      badge: triggerCount || undefined,
    },
    {
      section: "global_knowledge_graph",
      label: "Knowledge Graph",
      icon: <IconGlobe />,
      badge: countryCount || undefined,
    },
    {
      section: "sales_tracker",
      label: "Sales Tracker",
      icon: <IconChart />,
      badge: claimedCount || undefined,
    },
    {
      section: "settings",
      label: "Research Studio",
      icon: <IconBeaker />,
      badge: activeRunCount || undefined,
    },
  ];

  const initials = userName
    ? userName
        .split(/\s+/)
        .map((w) => w[0])
        .join("")
        .slice(0, 2)
        .toUpperCase()
    : null;

  return (
    <nav className="navRail">
      {/* brand mark */}
      <div className="nrBrand">
        <span className="nrBrandMark">K</span>
        <span className="nrLiveDot" />
      </div>

      {/* section navigation */}
      <div className="nrNav">
        {items.map((item) => (
          <button
            key={item.section}
            className="nrButton"
            data-active={activeSection === item.section}
            onClick={() => onSectionChange(item.section)}
            title={item.label}
          >
            {item.icon}
            <span className="nrLabel">{item.label}</span>
            {item.badge ? <span className="nrBadge">{item.badge}</span> : null}
          </button>
        ))}
      </div>

      {/* bottom actions */}
      <div className="nrBottom">
        <button className="nrButton nrRefresh" onClick={onRefresh} title="Refresh data">
          <IconRefresh />
          <span className="nrLabel">Refresh</span>
        </button>

        <div className="nrAccount" title={userName ?? "Account"}>
          {initials ? (
            <span className="nrAvatar">{initials}</span>
          ) : (
            <span className="nrAvatarIcon"><IconUser /></span>
          )}
        </div>
      </div>
    </nav>
  );
}
