"use client";

import { RichTextBlock } from "@/components/rich-text-block";
import { InsightCardData, InsightTone } from "@/types/view";

type Props = {
  data: InsightCardData | null;
  emptyTitle?: string;
  emptyBody?: string;
  sticky?: boolean;
};

function chipClass(tone: InsightTone = "neutral") {
  return `chip chip-${tone}`;
}

export function SelectionInfoCard({
  data,
  emptyTitle = "Nothing selected",
  emptyBody = "Select a node or map item to inspect the supporting detail.",
  sticky = true,
}: Props) {
  const className = `panel insightCard${sticky ? "" : " insightCardStatic"}`;

  if (!data) {
    return (
      <aside className={className}>
        <h2 className="panelTitle">{emptyTitle}</h2>
        <RichTextBlock className="bodyText bodyTextMuted" text={emptyBody} />
      </aside>
    );
  }

  return (
    <aside className={className}>
      {data.eyebrow ? <div className="panelEyebrow">{data.eyebrow}</div> : null}
      <h2 className="panelTitle">{data.title}</h2>
      {data.subtitle ? <p className="panelSubtitle">{data.subtitle}</p> : null}

      {data.badges?.length ? (
        <div className="chipWrap insightBadgeRow">
          {data.badges.map((badge) => (
            <span key={`${badge.label}-${badge.tone ?? "neutral"}`} className={chipClass(badge.tone)}>
              {badge.label}
            </span>
          ))}
        </div>
      ) : null}

      {data.metrics?.length ? (
        <div className="insightMetricGrid">
          {data.metrics.map((metric) => (
            <div key={`${metric.label}-${metric.value}`} className="insightMetricCard">
              <span>{metric.label}</span>
              <strong className={`tone-${metric.tone ?? "neutral"}`}>{metric.value}</strong>
            </div>
          ))}
        </div>
      ) : null}

      <div className="insightSectionStack">
        {data.sections?.map((section) => (
          <section key={section.title} className="textSection">
            <h2 className="sectionTitle">{section.title}</h2>
            {section.text ? <RichTextBlock className="bodyText" text={section.text} /> : null}
            {section.chips?.length ? (
              <div className="chipWrap">
                {section.chips.map((chip) => (
                  <span key={`${section.title}-${chip.label}`} className={chipClass(chip.tone)}>
                    {chip.label}
                  </span>
                ))}
              </div>
            ) : null}
            {section.linkHref ? (
              <a className="sourceLink" href={section.linkHref} target="_blank" rel="noreferrer">
                {section.linkLabel ?? "Open link"}
              </a>
            ) : null}
          </section>
        ))}
      </div>
    </aside>
  );
}
