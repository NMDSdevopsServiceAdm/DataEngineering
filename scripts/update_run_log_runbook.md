# Runbook: update run log flags

## Purpose
`scripts/update_run_log.sh` sets a flag to `True` for one run in the job role estimates archive run log (the tracking table) on S3. It never sets a flag back to `False`, except that selecting a run moves the month's selection. The run log has one row per run.

Run log location: `s3://<bucket>/domain=03_ind_cqc/dataset=01_filled_posts_08_archived_job_role_estimates_run_log/archive_date=<date>/run_number=<n>/`. The production bucket is `sfc-main-datasets`.

| Flag | What it records | Process |
|---|---|---|
| `approved` | We assessed the run and are satisfied it is accurate | [Approving a run](#approving-a-run-approved) |
| `selected_for_publication` | The one run chosen to publish this month | [Selecting a run for publication](#selecting-a-run-for-publication-selected_for_publication) |
| `promoted` | The run's estimates, metadata and run-log partitions are copied into the long-term archive bucket (set automatically, not chosen at the prompt) | [Promoting a run](#promoting-a-run-promoted) |
| `reconciled` | The public output's totals match the archived estimates (set automatically by Publication-Output; choosing it at the prompt only forces the flag) | [Reconciling a run](#reconciling-a-run-reconciled) |
| `locally_checked` | The refreshed Tableau file passed its local check | [Stub](#marking-a-run-locally-checked-locally_checked) |

The `tag` column is set by `scripts/cut_release_tag.sh`, which this script offers to run after a selection: see [Cutting the release tag](#cutting-the-release-tag-tag). `promoted_at` and `reconciled_at` are set automatically alongside `promoted` and `reconciled` respectively (the current time) and are not themselves flags you choose. Assessing a run is the review of its assessment data and has no flag of its own. Approving, selecting, promoting and reconciling are separate decisions and separate updates (see `projects/ARCHIVING_AND_PUBLICATION.md`).

## Prerequisites
Run these once per terminal session, from the DataEngineering repo root, in PowerShell:

1. Set the HOME environment variable (`aws-mfa` reads it to find your `.aws` folder, and Windows may not set it). This only applies to the current terminal, so repeat it in each new one:

```
$Env:HOME = $Env:USERPROFILE
```

2. Provide your MFA token (the temporary credentials last 12 hours):

```
aws-mfa --mfa-profile prod --token xxxxxx
```

3. Install the Python environment (first time, or after dependencies change):

```
uv sync
```

The script needs Git Bash (bash 4 or later), so run it from there. Steps 1 and 2 can be done in a separate PowerShell window: the credentials are saved in your `.aws` folder and shared. `aws` and `uv` must be on the Git Bash PATH. The AWS profiles the script uses must be set up as described in `WINDOWS_SETUP.md`.

## How the script works
Every flag uses the same script and the same prompts:

1. It asks for the **AWS profile** (Enter for the default, `run-log-updater`; use `non-prod` for a branch bucket) and the **bucket**. Press Enter at the bucket prompt only for production (`sfc-main-datasets`); otherwise type a branch bucket name, without `s3://` or a trailing `/`.
2. It copies the run log down and lists the runs archived this month (from the 1st): run number, archive date, the five flags and the tag. Nothing is changed yet.
3. It asks for the **flag** and the **run number**, then shows the four answers and asks `Are these correct?`.
4. It builds the update in `run_log_updated/` and prints each changed run **Before** and **After**.
5. Only after you answer `y` at the upload prompt are the changed runs' folders replaced on S3. Anything other than y/yes/n/no is asked again, and `n` deletes the updated copy and uploads nothing.

`run_log_original/` and `run_log_updated/` are created in the repo root and ignored by git; each run clears both first. To only look at the list, press Ctrl-C at the Flag prompt.

## Approving a run (`approved`)

### Use case
After a run has been assessed, record that we are satisfied it is accurate. Several runs can be approved in a month, for example the first run of the month and a rerun after more source data arrives. Approving does not publish anything and does not start any pipeline. A run must be approved before it can be selected for publication.

Do this when you have reviewed the run's assessment data and are satisfied with it. The assessment data's `archive_date` and `run_number` partitions show which run you assessed.

### Steps
1. From the repo root, in Git Bash, run `bash scripts/update_run_log.sh`.
2. Enter the AWS profile and the bucket.
3. Check the list: the run you assessed is there with `approved False`. A run from an earlier month is not listed but can still be approved by its run number.
4. Flag: `approved`. Run number: the run you assessed.
5. Check the four answers and answer `y`.
6. Check **Before** and **After**: the only difference is `approved: False` becoming `approved: True`, and the run number and archive date are the ones you meant.
7. Answer `y` to upload.

### Expected result
- `Done: approved is now True for run <n> in s3://<bucket>/.`
- On a branch bucket, one restore command is printed. On production none is printed.
- No question about starting anything.
- Running the script again and looking at the list shows the run with `approved True`.

### If something is wrong
- **Run number not found**: nothing is uploaded; check the number against the assessment data.
- **`Run number <n> is already approved`**: nothing to do.
- **The upload fails part way, or an answer was wrong**: see "Undoing an update". Approval cannot be removed by the script; restore the run.

## Selecting a run for publication (`selected_for_publication`)

### Use case
Choose the one run to publish this month. This is a separate decision from approving, made after at least one run is approved. It can pick any approved run, not necessarily the latest: if the first run of the month is approved and a later rerun is also approved and preferred, select the later one. If you selected the wrong run, select the right one: the selection moves.

Selecting a run also automatically copies it into the long-term archive bucket (see "Promoting a run" below — this step runs on its own, with no extra confirmation), lets you cut the release tag for it (see "Cutting the release tag" below), then lets you start the `Publication-Output` step function for it, which builds the public data (the publication summary behind the Tableau output and the T0, T1 and T2 data-download tables). The tag comes first and does not wait for `Publication-Output` or its reconciliation: it marks the run you chose to publish.

Rules:
- The run must be approved. Approve it first (see "Approving a run") if it is not.
- The run must have been archived on or after the 1st of the current month. Earlier runs are not listed and cannot be selected.
- At most one run is selected since the 1st. Selecting a different run clears the previous selection in the same update. Selections from earlier months are never changed.

### Steps
1. From the repo root, in Git Bash, run `bash scripts/update_run_log.sh`.
2. Enter the AWS profile and the bucket.
3. Check the list: the run you want shows `approved True`. Note which run, if any, already shows `selected_for_publication True`.
4. Flag: `selected_for_publication`. Run number: the run you want to publish.
5. Check the four answers and answer `y`.
6. Check **Before** and **After**:
   - First selection this month: one block, for your run, where only `selected_for_publication` changes from `False` to `True`.
   - Moving the selection: two blocks, your run (`False` to `True`) and the previously selected run (`True` to `False`).
7. Answer `y` at the upload prompt. A move shows `Upload 2 changed run(s)`, and both folders are replaced after this one confirmation.
8. The script automatically promotes the run (see "Promoting a run" below) — no prompt, it just runs. If this fails, it stops here; see that section's "If something is wrong".
9. The script prints the release tag it would cut (see "Cutting the release tag" below) and asks `Cut the release tag now?` (`Dry run the release tag now?` for a branch bucket). Answer `y` to cut it, or `n` to cut it yourself later.
10. The script offers to start `<workspace>-Publication-Output` (`main-Publication-Output` for `sfc-main-datasets`; `<branch>-Publication-Output` for a branch bucket `sfc-<branch>-datasets`). It shows the state machine name, its ARN and the input `{"run_number": <n>}`. Answer `y` to start it, or `n` to start it yourself later with that input. Nothing waits for it to finish.

### Expected result
- `Done: selected_for_publication is now True for run <n> in s3://<bucket>/.`
- For a move, a line `The selection moved, so these runs were changed: ...`.
- On a branch bucket, one restore command per changed run.
- The promotion step's own output (see "Promoting a run"), ending `Promoted run <n> (archived <date>) to s3://<workspace>-archive-promoted/.`.
- The run, commit, tag and tag message from `cut_release_tag.sh plan`, then, after you answer `y`, the result of the cut (see "Cutting the release tag").
- If you answered `y` at step 10, the execution's ARN and `Started.`. In the Step Functions console the new execution of `<workspace>-Publication-Output` has the input `{"run_number": <n>}`.

`Publication-Output` builds the public data from the run's prepared data, which `Publication-Assessment` stores per run. `Publication-Assessment` starts itself when the estimates pipeline succeeds in `main`; on a branch it must be started by hand with `{"run_number": <n>}`. The script does not check that the run was prepared.

### If something is wrong
- **`Run number <n> is not approved`**: nothing is uploaded. Approve the run first.
- **`Run number <n> was archived before <1st of the month>`**: nothing is uploaded. Only this month's runs can be selected; this includes a run that was selected in an earlier month.
- **`Run number <n> is already selected_for_publication`**: that run is already the selection; nothing to do.
- **Run number not found**: nothing is uploaded; pick a run from the list.
- **The upload fails part way**: the script names the runs it uploaded and the one that failed. A moved selection can be left with no run selected, or two runs selected. Restore every run it names (on a branch bucket, run the restore commands it prints; see "Undoing an update"), then run the script again.
- **`The selection is saved, but promotion did not complete`**: see "Promoting a run"'s "If something is wrong" — the selection itself does not need redoing.
- **`State machine ... was not found`, `could not list state machines`, or `could not start`**: the selection is already saved. The script prints `Start Publication-Output by hand with {"run_number": <n>}`; start `<workspace>-Publication-Output` with that input. A branch has the step function only once the branch has deployed it, and your profile needs permission to list and start state machines in that workspace.
- **`could not work out the release tag` or `the release tag was not fully cut`**: the selection is unaffected and the `Publication-Output` offer still follows. See "Cutting the release tag" to fix the cause and run it by hand.
- **`Publication-Output` starts but fails with `run_number <n> not found ... Available runs`**: the run was never prepared. The selection stays set. Start `<workspace>-Publication-Assessment` with `{"run_number": <n>}` (on main the `run-log-updater` role can't start it; use another profile with access), wait for it to finish, then start `Publication-Output` again with the same input.

## Promoting a run (`promoted`)

### Use case
Copy a run's estimates, metadata and run-log partitions into the long-term archive bucket
(`sfc-<workspace>-archive-promoted`), where they are kept indefinitely — the rolling archive in
`sfc-<workspace>-datasets` expires runs that were never promoted after about 2 months. This runs
**automatically** as part of selecting a run for publication (see "Selecting a run for
publication" above, step 8). `promoted` is not one of the flags `update_run_log.sh` lets you choose
directly — recover a failed attempt with the commands below instead, not by re-running the script.

### Steps (automatic, via selection)
No separate steps — this runs as part of "Selecting a run for publication" above. It:
1. Marks the run `promoted` in the run log and uploads that change, the same way every other flag
   is uploaded.
2. Copies the run's estimates partition into the long-term bucket, then its metadata partition,
   then its (now `promoted: True`) run-log partition, printing a count of files copied for each.

### Expected result
- One `Copied <n> object(s) for <prefix>/<partition>.` line per dataset (estimates, metadata,
  run log).
- `Promoted run <n> (archived <date>) to s3://<workspace>-archive-promoted/.`
- The run log in both buckets shows `promoted: True` for the run, with the same `promoted_at`
  timestamp.

### If something is wrong
- **`could not mark run <n> as promoted`**: the flag update itself failed, so nothing was copied —
  the run log does **not** yet show `promoted: True`. If the message says the run is "already
  promoted", there's nothing to do (a previous attempt already finished — check the long-term bucket
  to confirm). This also happens when you reselect a run that was promoted earlier: the script stops
  at this point and never offers the release tag or `Publication-Output`, so cut the tag by hand if the run has none (see "Cutting the release tag"; a run whose tag is already on its commit is accepted) and start `<workspace>-Publication-Output` by
  hand with `{"run_number": <n>}`. If it says the run is "not selected_for_publication", select it
  first. Otherwise, recover with "Recovering a promotion that failed before the flag was set" below.
- **`could not upload the promoted run log partition`**: the run log on S3 does **not** yet show
  `promoted: True` and nothing was copied. Restore the run with the restore command the script
  prints, then recover with "Recovering a promotion that failed before the flag was set" below.
- **`could not copy <prefix>/<partition>`**, **`copy of <prefix>/<partition> looks incomplete`** or
  **`could not list s3://<workspace>-archive-promoted/...`**: these all happen *after* the flag update
  already succeeded (the run log genuinely shows `promoted: True`). Finish the copy by hand instead:
  the script prints the exact `aws s3 cp ... --recursive --profile <profile>` command for the
  dataset that failed — run it, then re-check the file count matches between source and destination
  (see "Not sure whether a promotion fully finished" below). Then do the same for the datasets after
  it: the script stops at the first failure, so the estimates, metadata and run log are copied in that
  order and every later one still needs copying. If the first copy fails, check the profile can write
  to the long-term bucket.
- **`nothing found at s3://<bucket>/<prefix>/<partition>/`** or **`could not list s3://<bucket>/...`**:
  also after the flag update. The source partition has no files or could not be read, so check the
  run's partition in `<bucket>` exists before copying anything.
- **Not sure whether a promotion fully finished**: for each of the estimates, metadata and run log
  datasets, count the files in the run's partition in `<bucket>` and then in the long-term bucket. Replace
  `<profile>`, `<bucket>`, `<workspace>` and `<partition>` (`archive_date=<date>/run_number=<n>`), then run:

  ```bash
  aws s3 ls --recursive "s3://<bucket>/domain=03_ind_cqc/dataset=01_filled_posts_08_archived_job_role_estimates/<partition>/" --profile <profile> | wc -l
  aws s3 ls --recursive "s3://<workspace>-archive-promoted/domain=03_ind_cqc/dataset=01_filled_posts_08_archived_job_role_estimates/<partition>/" --profile <profile> | wc -l
  aws s3 ls --recursive "s3://<bucket>/domain=03_ind_cqc/dataset=01_filled_posts_08_archived_job_role_estimates_metadata/<partition>/" --profile <profile> | wc -l
  aws s3 ls --recursive "s3://<workspace>-archive-promoted/domain=03_ind_cqc/dataset=01_filled_posts_08_archived_job_role_estimates_metadata/<partition>/" --profile <profile> | wc -l
  aws s3 ls --recursive "s3://<bucket>/domain=03_ind_cqc/dataset=01_filled_posts_08_archived_job_role_estimates_run_log/<partition>/" --profile <profile> | wc -l
  aws s3 ls --recursive "s3://<workspace>-archive-promoted/domain=03_ind_cqc/dataset=01_filled_posts_08_archived_job_role_estimates_run_log/<partition>/" --profile <profile> | wc -l
  ```

  The numbers in each pair (bucket, then long-term bucket) should be the same, and the run log's should be 1.

### Recovering a promotion that failed before the flag was set
If the run log does **not** yet show `promoted: True` for this run (the flag update or its
upload failed), there is no automatic retry — run these by hand, in order, from the repo
root in Git Bash. Replace `<profile>`, `<bucket>` (the `sfc-<workspace>-datasets` bucket the run was
selected on), `<n>` and `<partition>` (`archive_date=<date>/run_number=<n>`, from the run's row in
the listing):

```bash
aws s3 sync "s3://<bucket>/domain=03_ind_cqc/dataset=01_filled_posts_08_archived_job_role_estimates_run_log/" run_log_original --profile <profile>
uv run python -m scripts.update_run_log --run_log_dir run_log_original --output_dir run_log_promoted --run_number <n> --flag promoted
aws s3 sync "run_log_promoted/<partition>/" "s3://<bucket>/domain=03_ind_cqc/dataset=01_filled_posts_08_archived_job_role_estimates_run_log/<partition>/" --delete --profile <profile>
aws s3 cp "s3://<bucket>/domain=03_ind_cqc/dataset=01_filled_posts_08_archived_job_role_estimates/<partition>/" "s3://<workspace>-archive-promoted/domain=03_ind_cqc/dataset=01_filled_posts_08_archived_job_role_estimates/<partition>/" --recursive --profile <profile>
aws s3 cp "s3://<bucket>/domain=03_ind_cqc/dataset=01_filled_posts_08_archived_job_role_estimates_metadata/<partition>/" "s3://<workspace>-archive-promoted/domain=03_ind_cqc/dataset=01_filled_posts_08_archived_job_role_estimates_metadata/<partition>/" --recursive --profile <profile>
aws s3 cp "s3://<bucket>/domain=03_ind_cqc/dataset=01_filled_posts_08_archived_job_role_estimates_run_log/<partition>/" "s3://<workspace>-archive-promoted/domain=03_ind_cqc/dataset=01_filled_posts_08_archived_job_role_estimates_run_log/<partition>/" --recursive --profile <profile>
rm -rf run_log_original run_log_promoted
```

The first `update_run_log.py` call still enforces the guard (fails if the run isn't
`selected_for_publication`, or if it's already `promoted`), so this is safe to run without first
checking the run log by hand.

## Reconciling a run (`reconciled`)

### Use case
Confirm the public output's total filled posts for a run matches its promoted archive's total, within a small tolerance, before the public output is considered correct. This runs **automatically** as the last stage of the `Publication-Output` state machine — you don't normally choose this flag yourself. The run must already be `promoted` for either path below to work, since the comparison reads the long-term archive.

### Steps (automatic, via Publication-Output)
No separate steps — this runs as the last stage of `Publication-Output` (started per "Selecting a run for publication" above, or by hand). It:
1. Waits for the run's run log partition to appear in the long-term archive bucket ("Check Promoted Archive"; promotion copies the run log last, so it marks a finished copy), rechecking on a fixed schedule (`reconciliation_polling_*` in `terraform/pipeline/step-function.tf`) before failing with `PromotionTimeout` if the run was never promoted.
2. Recomputes the public output's and the promoted archive's filled posts independently and compares each period within a small relative tolerance ("Compare Totals").
3. On a match, marks the run `reconciled` in the run log (recording `reconciled_at`). On a mismatch, the execution fails without changing the run log, and the pipeline's usual failure notification fires.

Don't approve or select runs, or run `cut_release_tag.sh` by hand, while `Publication-Output` is finishing: marking the run reconciled and writing the tag each rewrite that run's run log row from a copy read moments earlier, which could undo the other's change or a selection made in between. The script's own tag step runs before it offers `Publication-Output`, so it is not affected.

### Steps (manual, only to force the flag without the comparison)
Choosing this flag yourself skips the comparison entirely — only do this if the automated check is known to be wrong for a specific run (for example, a data correction already reconciled by hand). The run must already be `promoted`.
1. From the repo root, in Git Bash, run `bash scripts/update_run_log.sh`.
2. Enter the AWS profile and the bucket.
3. Flag: `reconciled`. Run number: the run to mark reconciled.
4. Check the four answers and answer `y`, then **Before**/**After** and answer `y` to upload.

### Expected result
- In the Step Functions console, `Publication-Output`'s "Check Promoted Archive" and "Compare Totals" steps both succeed.
- The run log shows `reconciled: True` for the run, with a `reconciled_at` timestamp.

### If something is wrong
- **`Publication-Output` fails with `PromotionTimeout`**: the run was never promoted, or promotion hasn't finished. Select/promote the run first (see "Promoting a run"), then start `Publication-Output` again with the same `run_number`.
- **`Publication-Output` fails with `PromotionCheckFailed`**: the check task itself failed, not just "not promoted yet" (for example a permissions error). Nothing is retried; see the task's CloudWatch log.
- **`Publication-Output` fails at "Compare Totals"**: one or more periods differ by more than the tolerance (the error lists them), or a period is missing from the archive. This needs investigating, not rerunning — the task's CloudWatch log prints both totals and the largest period difference.
- **`Run number <n> is not promoted`** (manual path): promote the run first.
- **`Run number <n> is already reconciled`**: nothing to do.

## Marking a run locally checked (`locally_checked`)
**Stub: process not yet written.**

- Use case: record that the analysts' check of the refreshed Tableau file passed.
- Who and when: to be decided. Nothing reads this flag yet.
- Steps: to be written. The script can already set the flag by following the prompts in "How the script works" with Flag `locally_checked`.

## Cutting the release tag (`tag`)

### Use case
Tag the exact code that produced the selected run's data as `vYYYY.MM.N` (MM is the month before the run's archive date, N the next free number for that month) and move that commit's CHANGELOG entries under the new version. `update_run_log.sh` offers this straight after a selection and its promotion, before it offers `Publication-Output` (step 9 of "Selecting a run for publication"); it runs `scripts/cut_release_tag.sh` from your machine, with your own git and GitHub login.

The run must be `selected_for_publication` and have no `tag` other than one already on its own commit (a rerun). It does not need to be `reconciled`: the tag marks the run you chose to publish, and a reconciliation failure is fixed separately. The tag is permanent: if the run is later replaced or fails reconciliation, the tag stays. A replacement run on a different commit for the same data month gets the next number (`.1` after `.0`), a run on the same commit reuses the tag, and a later data month starts again at `.0`. The tag goes on the run's `commit_sha`, with the message "Codebase used for data as at <Month YYYY>". The script then writes the tag to the run log, and opens a PR from `rel-<tag>` that moves into the new version only the `[Unreleased]` entries present at that commit. Entries merged to `main` since stay under `[Unreleased]`.

The tag marks the code that produced the data, so the tagged commit's own CHANGELOG still lists its entries under `[Unreleased]`: the `[vYYYY.MM.N]` heading exists only on `main`, from the PR, after the tag. Read the version's entries from `main`'s CHANGELOG, not from the CHANGELOG at the tag.

### Before you start
- `gh auth status` shows you signed in, and `git push` to `origin` works (origin must be `NMDSdevopsServiceAdm/DataEngineering`; a real release refuses anything else).
- Run from a clone whose `main` is up to date: the script and its Python run from your working copy.
- A branch bucket (`sfc-<branch>-datasets`) only ever dry-runs: every tag, S3, branch and PR change is printed, not made. A real release needs `sfc-main-datasets`.

### Steps
1. Select the run as in "Selecting a run for publication". After the promotion output the script prints the run, its commit, the tag it would cut and the tag message. Check the commit and tag are what you expect.
2. Answer `y` at `Cut the release tag now?`. It pushes the tag, writes the tag to the run log, pushes `rel-<tag>` and opens the PR. It takes about a minute and does not wait for anything. It runs before `Publication-Output` is offered, so its run log write cannot race that execution's `reconciled` write (both rewrite the same row); if you run it by hand later, do it while no execution is finishing.
3. Merge the `rel-<tag>` PR once its checks pass, soon after it opens. It rewrites the `[Unreleased]` section, and GitHub's merge does not use the CHANGELOG union merge, so another PR that edits those lines can conflict. If it does, close the PR and delete the `rel-<tag>` branch, then run the by-hand commands below again: the script reuses the tag and rebuilds the PR from the current `main`.

The PR moves only entries whose text matches exactly at the run's commit. An entry reworded since stays under `[Unreleased]` and ships with the next version. The first PR may also show blank-line changes in `[Unreleased]`.

### Running it by hand
After answering `n`, or after a failure, from the repo root in Git Bash (`<n>` is the run number; use your branch bucket and leave `RELEASE_DRY_RUN` `true` to test):

```bash
export RELEASE_RUN_NUMBER=<n> RELEASE_BUCKET=sfc-main-datasets RELEASE_DRY_RUN=false AWS_PROFILE=run-log-updater
bash scripts/cut_release_tag.sh plan
bash scripts/cut_release_tag.sh cut
```

`plan` prints what `cut` will do and changes nothing; `cut` stops if the plan differs from the one `plan` just wrote.

### Expected result
- The tag exists on GitHub, pointing at the run's commit (`git ls-remote --tags origin <tag>`).
- The run's `tag` in the run log shows it (`update_run_log.sh` lists it).
- A PR `Release <tag>: CHANGELOG` is open.

### If something is wrong
- **`not selected_for_publication` / `already tagged`**: the plan stops and nothing changes. Fix the run's state first.
- **`Commit <sha> is not on main` / `is not in this clone`**: the run was archived from a branch, or your clone is missing the commit (`git fetch origin`). Nothing is tagged.
- **`origin is '<url>', not NMDSdevopsServiceAdm/DataEngineering`**: fix the remote before a real release.
- **`A local tag <tag> already points at ...`** or **`A local branch rel-<tag> already exists`**: a leftover from an earlier attempt; delete it with the command in the message.
- **`The plan changed since it was shown`** or **`The plan was made for ...`**: another tag or run log change landed between `plan` and `cut`, or the plan was for a different bucket or mode. Run `plan` again.
- **`nothing to release` / `no Unreleased entries`**: none of the run's commit's `[Unreleased]` entries are still unreleased on `main`, so there is nothing to move. The plan stops before anything is pushed.
- **`The run's commit_sha ... is not a full commit SHA`**: the run log's `commit_sha` is empty or shortened. Nothing is tagged.
- **The push is refused**: your account cannot push tags or branches to `origin`. Nothing after the refused step ran.
- **`<tag> is already in main's CHANGELOG`**: the PR was merged, so everything is done and nothing is pushed.
- **It fails after the tag was pushed**: run it again. A run whose tag is already on its own commit is accepted, so it reuses the tag, skips any step already done (the tag push, the run log write, an existing `rel-<tag>` branch, an existing PR) and carries on.
- **A tag was cut for the wrong run**: delete the tag (`git push origin :refs/tags/<tag>` and `git tag -d <tag>`), close the PR and delete `rel-<tag>`, restore the run's run log partition (see "Undoing an update"), and start again.

### Testing safely
Use a branch bucket first: it only prints the changes. A branch bucket is seeded by CI only when a push touches archive or publication code, so otherwise copy the run's partition of each of the estimates, estimates metadata and run log datasets from `sfc-main-datasets` into `sfc-<branch>-datasets` (down with the `run-log-updater` profile, up with `non-prod`; they are different accounts). The run must be approved, archived this month and not selected on the branch bucket; reset a copied run's `selected_for_publication`, `promoted` and `tag` if it is not.

1. By hand, dry run: `RELEASE_RUN_NUMBER=<n> RELEASE_BUCKET=sfc-<branch>-datasets RELEASE_DRY_RUN=true AWS_PROFILE=non-prod`, then `plan` and `cut` as above. `plan` shows a tag, commit and message even though the run is not reconciled; `cut` prints `DRY RUN:` lines for the tag push, the run log `cp` and `sync`, the branch push and the PR; afterwards `git tag -l <tag>` and `git ls-remote origin rel-<tag>` show nothing and the run log still has no tag.
2. Through `update_run_log.sh` on the branch bucket: select the run, answer `y` to both questions (the first reads `Dry run the release tag now?`). After the promotion output the plan block and the question appear, then the `DRY RUN:` lines after your `y`; the `Publication-Output` offer follows whether or not the tag step succeeded. Answering `n` prints the by-hand commands; making the plan fail (for example pointing `origin` at a missing repository) prints `The selection is saved` and exits with status 1.
3. The first real run is on `sfc-main-datasets`, watched: confirm the tag and its commit, the PR diff (only `[Unreleased]` entries moved under `## [<tag>] - dd/mm/yyyy`), `tag` in the run log, and the dev deploy CircleCI starts for `rel-<tag>`.

## If something is wrong (any flag)
- **Flag unsupported**: only `approved`, `selected_for_publication`, `reconciled` and `locally_checked` are accepted at the prompt — `promoted` is set automatically, not chosen directly (see "Promoting a run").
- **Run number not found, or the flag is already True**: the script stops with an error and uploads nothing. Fix the input and run it again.
- **You answer `n` at the upload prompt (or the update fails)**: the change is reverted. `run_log_updated/` is deleted, `run_log_original/` is left as it was, and nothing is uploaded.
- **Could not authenticate**: repeat the prerequisites (the MFA credentials have expired after 12 hours).
- **No run log found in the bucket**: check the bucket name. Nothing is uploaded.
- **The upload fails part way**: the script names the runs it had already uploaded and the one that failed. The failed run's S3 folder may hold two parquet files, the old one and the new one, which reads as two rows for the run. A rerun then fails with "matches multiple rows". Restore every run it names (on a branch bucket, run the restore commands it prints), then run the script again from the start (see "Undoing an update").

## Undoing an update
- **Branch bucket**: after a successful upload the script prints one restore command per changed run (an `aws s3 cp --recursive` then `aws s3 sync --delete` from `run_log_original/`). Run all of them to put the runs back as they were. **Run them before running the script again**: every run replaces `run_log_original/`, and restoring from a later copy puts the changed value back. A failed upload prints the same commands for every run it uploaded and for the one that failed, which may be half written. Undoing does not stop a step function execution that was already started.
- **Production bucket (`sfc-main-datasets`)**: the bucket is versioned. No restore command is printed; restore the previous version of each changed run's parquet file from S3 object versions.

## Testing safely
Trial runs MUST use a branch dataset bucket, never production. CircleCI copies the production run log into each branch's dataset bucket (the `copy-main-data` job), so enter the branch bucket name at the Bucket prompt. Check that the bucket prompt shows the branch bucket before confirming. Each push wipes the branch run log again, so a branch run log used for selection tests (which needs several runs this month) has to be seeded again after CI.
