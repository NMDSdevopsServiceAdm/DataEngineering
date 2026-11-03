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

One card per independently-workable piece, roughly a PR's worth.

Label cards by dependency tier so "Depends on" can name them:

- **Tier** = 1 + the highest tier among a card's dependencies; no dependencies = tier 1. Tier 1 cards can
  start immediately, tier 2 cards are unblocked by tier 1, and so on. A card only depends on lower tiers.
- **Label** = a prefix (an uppercase letter or two from the plan's subject) + the tier, plus a lowercase
  letter only when a tier has several cards: `E1`, `E2a`, `E2b`, `E3`. A tier with one card has no letter.
- Letters let a dependency be specific: `E3a` may need only `E2a`, while `E3b` needs every tier 2 card.
- Order cards by tier, then letter.

State the prefix in the lead-in sentence (see Output); if it's unclear what it should be, ask.

## 3. Ask the open questions

Before drafting, settle what the plan leaves open so cards carry decisions, not "Confirm during scoping"
bullets. List every open choice (limits, thresholds, location, failure behaviour, naming, scope
boundaries), then ask in chat, one question at a time, with a recommended answer where there is one. Stop
asking if the user says to park the rest.

- Fold each answer into the card's TODO, Scope or Where as fact.
- Keep "Confirm during scoping: ..." only for what can't be known yet: it depends on an earlier card's
  findings, on data not yet inspected, or on someone outside the conversation. Name which ("depends on
  E1's findings").
- The Q&A comes before any card output; the text limits in Output apply to the cards themselves.

## 4. Card format

Each card is two code blocks, one after the other, under a plain line saying which card it is
(`Card E2a`). Two blocks because Trello's title and description are separate fields.

Title block — one plain line, sentence case, no markdown (Trello's title field doesn't render it):

````
```
E1 - Add Excel dependencies and decide where the workbook jobs live
```
````

Description block — markdown, sections in this order:

````
```
/new-ticket <trello-number> Add Excel dependencies and decide where the workbook jobs live

**TODO:** <What to build or change, 2 to 3 sentences, naming the real columns/datasets/outputs.>

Where: <path or location, only if the plan or user gave one; otherwise delete this line>


**The problem to solve:** One or two sentences: the problem and what this card settles.

**Scope:**
- A concrete piece of work, one line each.
- Decisions made in step 3, stated as fact.
- "Confirm during scoping: ..." only for genuinely unknowable items (step 3).

**Depends on:** E1 - Add Excel dependencies and decide where the workbook jobs live

**Notes:** Only for a caveat the reader would otherwise miss.
```
````

- **First line** is the command the developer pastes into Claude to start the ticket:
  `/new-ticket <trello-number> <title>`. Write `<trello-number>` literally — Claude can't know it — and
  repeat the card title after it without the label, shortened if it isn't already extremely short.
- **TODO** tells the developer who picks the card up exactly what is wanted: one concrete, actionable
  statement of 2 to 3 sentences. Name the real columns/datasets/outputs; plain words, no card labels or
  jargon. It restates what Scope implies and adds no new work. Example: "Add unfiltered sum of CT posts
  columns (`col_name` to be summed) to `dataset_name_assessments`."
- **Where** is an optional line under TODO, in its own paragraph, for where the change applies. Include
  it only when the plan or user says; omit it otherwise — never guess, as the developer may scope it
  differently.
- **Depends on** names each card by label and title. "Nothing" if it can start immediately. One
  dependency stays on the line; two or more become a `-` bullet list under the label, one card per bullet.
  Name only the cards that actually block this one. For a whole tier write "all tier 2 cards (E2a-E2c)",
  and for everything before it "all earlier cards", instead of listing every one. A conditional dependency gets its
  condition in brackets after the card ("(only if ...)"); a short reason may go there too ("(so each run
  is tracked)").
- Omit **Notes** when there's nothing to say; never pad it.
- One line per paragraph and per bullet — don't hard-wrap. Bullets nest one level when items group under
  a heading line (e.g. roles under "by role:"); don't flatten a grouping into a flat list.
- Card references inside Scope text ("#5", "step 6") become labels too (`E3a`).
- Trello can't resolve references from the repo or Claude's memory, so replace `[[wiki links]]`, memory
  file names and repo-relative links with plain words ("the design doc") or a backticked path. Plain
  names for people are fine. External `https://` links are fine as markdown links.
- No tables, no HTML.

## Breakdown extras

Around the cards, each in its own code block under a plain line naming it (so it copies cleanly):

- **Board phases** (before the first card) — one suggested grouping on a single line, e.g.
  `Foundation (E1) → Public download (E2a-E2c) → Monthly jobs (E3a-E3b)`. Follows the tiers. Only when it
  helps the board read well.
- **Overlap edits** (after the cards) — only when the plan or the user says existing Trello cards overlap
  these. One line per card to edit: `<existing card> - <the one-line change that avoids double-counting>`.
  Never guess at overlap with cards you haven't been told about; omit the block if there is none.
- **Dependency overview** (last) — an ASCII graph of the cards. Build it from the finished **Depends on**
  lines, not separately, so the two can't disagree, with tiers running left to right, and end the block
  with one line naming the tier 1 cards (those that can start immediately). Cards depending on a whole tier
  or on all earlier ones are drawn as one `all tier 2 cards ---> E3b` or `all earlier cards ---> E4` edge, not
  one arrow each. A conditional dependency is drawn with a dashed `- - ->` and the condition beside it.

## Single card

Same two code blocks and section order as step 4, with these differences:

- No label and no prefix — the title is just the title. The "Card ..." line is replaced by plain
  `Title:` and `Description:` lines above the two blocks.
- Run the step 3 question round first for what's open (limits, thresholds, behaviour on failure, where
  it lives), only the few that would change the work.
- **Scope** holds only what the idea states or directly implies — or, when a plan exists, what the plan
  states for this one item (never other items) — plus the answers from the question round. Whatever is
  still genuinely unknowable goes in as a "Confirm during scoping: ..." bullet. Don't add tests, logging,
  acceptance criteria or other work the idea didn't mention.
- **TODO** comes first and holds what the idea (or the plan, for this item) states plus the question-round
  answers. Omit **Where** unless stated or answered.
- **The problem to solve** restates the idea and why it matters; it doesn't assert how things currently
  behave unless the user said so.
- **Depends on** is "Nothing" unless the user — or the plan, for this item — named a dependency. Never
  invent one. A lone card has no labels, so name any dependency by its title.
- Skip **Notes** unless the user or the plan gave a caveat for this item.

## Paste-only: never touch Trello

Claude has no Trello access, so the user pastes the cards in by hand. Never call a Trello API or MCP tool,
never create, move or label a card, and never ask for a board link, card URL, API key or token — ticket
details come from what the user types. If asked to add a card to the board or return its link, say you
can't reach Trello in the lead-in sentence (see Output), then give the paste-ready blocks: title block
into the card title, description block into the card description.

## Output

In chat. Apart from the blocks, the only text allowed is:

- **Label lines** directly above a block — `Card E2a`, `Title:`, `Description:`, or a block's name. Labels,
  not prose.
- **One lead-in sentence** before the first block. It states the prefix (breakdown) and, if the user asked
  to add the card to the board or get a link, that you can't reach Trello.
- **One closing line** after the last block, only when there is something for the user to check: plan
  references you replaced, or a plan that hints at existing cards without naming them (so no overlap
  block).

No other commentary or summary.

If the user asks for it saved, also write the same blocks to `trello-cards-<ticket|slug>.md` at the
worktree root — the ticket number from the branch name if it parses, else a short slug of the subject.
The chat output still appears in full. Leave the file untracked; it's theirs to paste from and delete.
Never commit it.
