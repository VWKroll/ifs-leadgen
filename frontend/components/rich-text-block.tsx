type Props = {
  text?: string | null;
  emptyText?: string;
  className?: string;
};

type TextBlock =
  | { type: "paragraph"; lines: string[] }
  | { type: "list"; items: string[] };

function normalizeMultilineText(text: string): string[] {
  return text
    .replace(/\r\n/g, "\n")
    .split("\n")
    .map((line) => line.replace(/\s+/g, " ").trim());
}

function parseTextBlocks(text?: string | null): TextBlock[] {
  if (!text) return [];

  const lines = normalizeMultilineText(text);
  const blocks: TextBlock[] = [];
  let paragraphLines: string[] = [];
  let listItems: string[] = [];

  function flushParagraph() {
    if (!paragraphLines.length) return;
    blocks.push({ type: "paragraph", lines: paragraphLines });
    paragraphLines = [];
  }

  function flushList() {
    if (!listItems.length) return;
    blocks.push({ type: "list", items: listItems });
    listItems = [];
  }

  for (const line of lines) {
    if (!line) {
      flushParagraph();
      flushList();
      continue;
    }

    const bulletMatch = line.match(/^([-*•]|\d+\.)\s+(.*)$/);
    if (bulletMatch) {
      flushParagraph();
      listItems.push(bulletMatch[2]);
      continue;
    }

    flushList();
    paragraphLines.push(line);
  }

  flushParagraph();
  flushList();

  return blocks;
}

export function RichTextBlock({
  text,
  emptyText = "No supporting text is available yet.",
  className,
}: Props) {
  const blocks = parseTextBlocks(text);

  if (!blocks.length) {
    return (
      <div className={className}>
        <p>{emptyText}</p>
      </div>
    );
  }

  return (
    <div className={className}>
      {blocks.map((block, index) =>
        block.type === "paragraph" ? (
          <p key={`paragraph-${index}`}>{block.lines.join(" ")}</p>
        ) : (
          <ul key={`list-${index}`}>
            {block.items.map((item, itemIndex) => (
              <li key={`item-${index}-${itemIndex}`}>{item}</li>
            ))}
          </ul>
        ),
      )}
    </div>
  );
}
