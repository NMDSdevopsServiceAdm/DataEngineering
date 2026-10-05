---
name: trello-cards
description: Use when the user wants Trello cards drafted — "write Trello cards for this plan", "break this down into cards", "draft a card for X", "/trello-cards". Turns a design/plan/memory file into a multi-card breakdown, or a one-line idea into a single card, as paste-ready text. Claude can't reach Trello, so this never calls it; the user pastes the cards in by hand.
---

# Drafting Trello cards

Turns a plan into cards in this repo's card format, laid out so each one pastes into Trello with no
re-formatting. The user copies from the code blocks below, so every card goes in code blocks — never as
rendered chat markdown, which loses its formatting on copy.

## 1. Identify the input and mode

- **Breakdown** — a plan file path (usually a ticket memory file) or pasted plan text, and the user wants
  several cards or the whole plan: draft several cards (step 2 onwards).
- **Single card** — the user wants one card, whether from a bare one-line idea or for one item of a plan
  that exists: draft one card (see "Single card" below).

If neither was given, ask in chat what to draft cards for.

## 2. Split into cards (breakdown only)

One card per independently-workable piece, roughly a PR's worth. Order by dependency, so a card only
depends on earlier ones.

Give every card a short label so "Depends on" can name cards: an uppercase letter or two taken from the
plan's subject plus the card's position, e.g. `E1`, `E2` for an Excel plan. State the prefix in the
lead-in sentence (see Output); if it's unclear what it should be, ask.

## 3. Card format

Each card is two code blocks, one after the other, under a plain line saying which card it is
(`Card E1`). Two blocks because Trello's title and description are separate fields.

Title block — one plain line, sentence case, no markdown (Trello's title field doesn't render it):

````
```
E1 - Add Excel dependencies and decide where the workbook jobs live
```
````

Description block — markdown, sections in this order:

````
```
**Summary:** One or two sentences: the problem and what this card settles.

**Scope:**
- A concrete piece of work, one line each.
- Open choices as "Confirm during scoping: ..." rather than decided for the user.

**Depends on:** E1 - Add Excel dependencies and decide where the workbook jobs live

**Notes:** Only for a caveat the reader would otherwise miss.
```
````

- **Depends on** names each card by label and title. "Nothing" if it can start immediately. One
  dependency stays on the line; two or more become a `-` bullet list under the label, one card per bullet.
  For "all earlier cards", write that instead of listing every one. A conditional dependency gets its
  condition in brackets after the card ("(only if ...)"); a short reason may go there too ("(so each run
  is tracked)").
- Omit **Notes** when there's nothing to say; never pad it.
- One line per paragraph and per bullet — don't hard-wrap. Bullets nest one level when items group under
  a heading line (e.g. roles under "by role:"); don't flatten a grouping into a flat list.
- Card references inside Scope text ("#5", "step 6") become labels too (`E5`).
- Trello can't resolve references from the repo or Claude's memory, so replace `[[wiki links]]`, memory
  file names and repo-relative links with plain words ("the design doc") or a backticked path. Plain
  names for people are fine. External `https://` links are fine as markdown links.
- No tables, no HTML.

## Breakdown extras

Around the cards, each in its own code block under a plain line naming it (so it copies cleanly):

- **Board phases** (before the first card) — one suggested grouping on a single line, e.g.
  `Foundation (E1) → Public download (E2-E4) → Monthly jobs (E5-E6)`. Only when it helps the board read
  well.
- **Overlap edits** (after the cards) — only when the plan or the user says existing Trello cards overlap
  these. One line per card to edit: `<existing card> - <the one-line change that avoids double-counting>`.
  Never guess at overlap with cards you haven't been told about; omit the block if there is none.
- **Dependency overview** (last) — an ASCII graph of the cards. Build it from the finished **Depends on**
  lines, not separately, so the two can't disagree, and end the block with one line naming the cards
  that can start immediately. Cards depending on all earlier ones are drawn as one `all earlier cards ---> E9` edge, not
  one arrow each. A conditional dependency is drawn with a dashed `- - ->` and the condition beside it.

## Single card

Same two code blocks and section order as step 3, with these differences:

- No label and no prefix — the title is just the title. The "Card ..." line is replaced by plain
  `Title:` and `Description:` lines above the two blocks.
- **Scope** holds only what the idea states or directly implies — or, when a plan exists, what the plan
  states for this one item (never other items). Everything left open (limits, thresholds, behaviour on
  failure, where it lives) goes in as a "Confirm during scoping: ..." bullet — only the few that would change the work (about four at most). Don't add tests, logging,
  acceptance criteria or other work the idea didn't mention.
- **Summary** restates the idea and why it matters; it doesn't assert how things currently behave unless
  the user said so.
- **Depends on** is "Nothing" unless the user — or the plan, for this item — named a dependency. Never
  invent one. A lone card has no labels, so name any dependency by its title.
- Skip **Notes** unless the user or the plan gave a caveat for this item.

## Paste-only: never touch Trello

Claude has no Trello access, so the user pastes the cards in by hand. Never call a Trello API or MCP tool,
never create, move or label a card, and never ask for a board link, card URL, API key or token — ticket
details come from what the user types. If asked to add a card to the board or return its link, say you
can't reach Trello in the lead-in sentence (see Output), then give the paste-ready blocks: title block
into the card title, description block into the card description.

## Size

Add one line to every description, after **Depends on** and before **Notes**: `**Size:** N`, where N is
a Fibonacci point: 1, 2, 3, 5 or 8.

- 1 trivial (a config or doc tweak), 2 small and well understood, 3 a moderate change in one place,
  5 several files or code plus infrastructure, 8 large or with real unknowns. "Confirm during scoping"
  bullets alone don't make a card an 8; the unknowns have to be large.
- Size is relative effort judged from the plan or idea alone — a rough guess, not a commitment; the
  closing line says so (see Output).
- In a breakdown, a card that would be bigger than 8 is split into smaller cards instead; the closing line
  says which one was split. A single card is never split — if it's over 8, size it 8 and say so in the
  closing line.

To stop sizing cards, delete this section and the size mentions in Output's closing line.

## Output

In chat. Apart from the blocks, the only text allowed is:

- **Label lines** directly above a block — `Card E1`, `Title:`, `Description:`, or a block's name. Labels,
  not prose.
- **One lead-in sentence** before the first block. It states the prefix (breakdown) and, if the user asked
  to add the card to the board or get a link, that you can't reach Trello.
- **One closing line** after the last block, always present when sizes are shown. It says sizes are rough
  guesses and adds anything else the user should check: a split or over-8 card, plan references you
  replaced, or a plan that hints at existing cards without naming them (so no overlap block).

No other commentary or summary.

If the user asks for it saved, also write the same blocks to `trello-cards-<ticket|slug>.md` at the
worktree root — the ticket number from the branch name if it parses, else a short slug of the subject.
The chat output still appears in full. Leave the file untracked; it's theirs to paste from and delete.
Never commit it.
