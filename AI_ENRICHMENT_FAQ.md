# AI Enrichment FAQ

## When Does AI Enrichment Run?

### Automatic Schedule
The GitHub Action runs **daily at 6am UTC** (1am EST / 10pm PST):
```yaml
schedule:
  - cron: '0 6 * * *'
```

### Manual Trigger
You can also trigger it manually:
1. Go to GitHub → Actions tab
2. Click "Update experience data" workflow
3. Click "Run workflow" button
4. Select branch (main) and click "Run workflow"

### Current Status
Your events **do NOT have AI enrichment yet** because:
- The workflow hasn't run since you added the `OPENAI_API_KEY` secret
- Current events: 2 total, 0 with AI enrichment

## Do You Need to Run It Again?

**YES** - You should trigger the workflow manually to:
1. Apply AI enrichment to existing events
2. Add geocoding with free Nominatim
3. Verify the OpenAI API key works correctly

### How to Trigger Manually

**Option 1: GitHub UI**
1. Go to: https://github.com/Auerlian/RostaScrapers/actions
2. Click "Update experience data" workflow
3. Click "Run workflow" → "Run workflow"

**Option 2: Command Line (if you have GitHub CLI)**
```bash
gh workflow run scrape.yml
```

**Option 3: Local Test First (Recommended)**
```bash
# Test locally to verify everything works
export OPENAI_API_KEY="your-key-here"
python run_pipeline.py run

# Check results
python -c "
from src.storage.store import CanonicalStore
store = CanonicalStore()
events = store.load_events()
with_ai = sum(1 for e in events if hasattr(e, 'description_ai') and e.description_ai)
print(f'{with_ai}/{len(events)} events enriched')
"
```

## Will the Action Throttle the API?

### Short Answer: NO - It's Well Optimized

The pipeline has **multiple layers of protection** against excessive API usage:

### 1. Smart Caching (Primary Protection)
```
Cache Key = hash(source_hash + prompt_version + model)
```

**How it works:**
- First run: Calls OpenAI for each event (~$0.001 per event)
- Subsequent runs: Uses cached results (FREE, instant)
- Cache invalidation: Only when event content changes

**Example:**
- Day 1: 100 events → 100 API calls → $0.10
- Day 2: 100 events (95 unchanged, 5 new) → 5 API calls → $0.005
- Day 3: 100 events (all unchanged) → 0 API calls → $0.00

### 2. Conditional Enrichment
The enricher only processes events that:
- Have a `description_clean` field (skips empty descriptions)
- Don't already have AI enrichment (unless source changed)
- Pass validation checks

### 3. Error Handling & Retries
```python
try:
    enriched = enricher.enrich_event(event)
except Exception as e:
    # Log error, continue with original event
    # NO infinite retries, NO cascading failures
    enriched = event
```

### 4. Rate Limiting (Built-in)
OpenAI SDK includes automatic rate limiting:
- Respects API rate limits
- Exponential backoff on errors
- Timeout protection (30 seconds default)

### 5. Cost Controls
**Recommended OpenAI settings:**
1. Go to: https://platform.openai.com/account/limits
2. Set "Monthly budget" (e.g., $10/month)
3. Enable "Email notifications" at 50%, 75%, 90%
4. Set "Hard limit" to stop API calls when budget reached

## Expected API Usage

### Initial Run (All Events New)
| Events | API Calls | Cost (gpt-4o-mini) | Time |
|--------|-----------|-------------------|------|
| 10     | 10        | $0.01             | ~30s |
| 50     | 50        | $0.05             | ~2m  |
| 100    | 100       | $0.10             | ~5m  |
| 500    | 500       | $0.50             | ~25m |

### Daily Runs (10% New/Changed)
| Events | API Calls | Cost | Time |
|--------|-----------|------|------|
| 10     | 1         | $0.001 | ~3s |
| 50     | 5         | $0.005 | ~15s |
| 100    | 10        | $0.01  | ~30s |
| 500    | 50        | $0.05  | ~2m |

### Monthly Cost Estimate
Assuming daily runs with 10% churn:
- 100 events: ~$3/month
- 500 events: ~$15/month
- 1000 events: ~$30/month

## Monitoring API Usage

### Check OpenAI Dashboard
1. Go to: https://platform.openai.com/usage
2. View daily API costs
3. Monitor request counts
4. Check rate limit status

### Check Local Cache
```bash
# Count cached enrichments
ls cache/ai/ | wc -l

# View cache size
du -sh cache/ai/

# View a cached enrichment
cat cache/ai/*.json | jq
```

### Check GitHub Action Logs
1. Go to: https://github.com/Auerlian/RostaScrapers/actions
2. Click on latest workflow run
3. Expand "Run pipeline" step
4. Look for:
   - "✅ OpenAI API key found, enabling AI enrichment"
   - "AI enrichment complete: X succeeded, Y failed"
   - "Exported X events to exports/events.csv"

### Check Enrichment Status
```bash
# Run locally to check enrichment status
python -c "
from src.storage.store import CanonicalStore
store = CanonicalStore()
events = store.load_events()

total = len(events)
with_ai = sum(1 for e in events if hasattr(e, 'description_ai') and e.description_ai)
with_tags = sum(1 for e in events if hasattr(e, 'tags') and len(e.tags) > 0)

print(f'Total events: {total}')
print(f'With AI enrichment: {with_ai} ({with_ai/total*100:.1f}%)')
print(f'With tags: {with_tags} ({with_tags/total*100:.1f}%)')
"
```

## What Happens During Enrichment?

### Pipeline Flow
```
1. Extract → Scrape provider websites
2. Normalize → Convert to canonical format
3. Enrich → AI + Geocoding
   ├─ Geocoding: Add lat/lng (Nominatim/Mapbox)
   └─ AI Enrichment: Enhance descriptions
4. Sync → Merge with existing data
5. Export → Generate CSVs
```

### AI Enrichment Process
For each event:
1. **Check cache** - Is this event already enriched?
   - YES → Use cached result (instant, free)
   - NO → Continue to step 2
2. **Validate input** - Does event have description_clean?
   - NO → Skip enrichment
   - YES → Continue to step 3
3. **Call OpenAI API** - Generate enriched content
   - Model: gpt-4o-mini
   - Timeout: 30 seconds
   - Cost: ~$0.001 per event
4. **Parse response** - Extract structured data
   - description_ai, summaries, tags, metadata
5. **Cache result** - Save for future runs
6. **Apply to event** - Update event record

### Error Handling
If anything fails:
- ❌ API timeout → Use original description
- ❌ Invalid JSON → Log error, continue
- ❌ Network error → Retry once, then skip
- ❌ Rate limit → Wait and retry
- ✅ Pipeline continues regardless

## Recommendations

### For Your First Run
1. **Test locally first** (optional but recommended):
   ```bash
   export OPENAI_API_KEY="your-key-here"
   python run_pipeline.py run
   ```

2. **Trigger workflow manually**:
   - Go to GitHub Actions
   - Run "Update experience data" workflow
   - Watch the logs

3. **Verify results**:
   - Check exports/events.csv for AI-enriched descriptions
   - Verify cache/ai/ has cached enrichments
   - Check OpenAI dashboard for API usage

### For Ongoing Monitoring
1. **Set OpenAI budget limit** ($10-20/month is plenty)
2. **Enable email notifications** at 75% budget
3. **Check cache directory** is committed to git
4. **Review enrichment quality** monthly
5. **Monitor GitHub Action logs** for errors

### Cost Optimization Tips
1. ✅ **Keep cache in git** - Avoid re-enriching on every run
2. ✅ **Use gpt-4o-mini** - 10x cheaper than gpt-4, good quality
3. ✅ **Run daily, not hourly** - Most events don't change that often
4. ✅ **Set OpenAI budget limits** - Prevent surprise bills
5. ✅ **Monitor cache hit rate** - Should be >90% after first run

## Troubleshooting

### "No AI enrichment applied"
**Check:**
- Is `OPENAI_API_KEY` set in GitHub Secrets?
- Did the workflow run successfully?
- Check logs for "✅ OpenAI API key found"
- Verify events have `description_clean` field

### "API costs too high"
**Solutions:**
- Verify caching is working (check cache/ai/ directory)
- Ensure cache directory is committed to git
- Reduce run frequency (weekly instead of daily)
- Set OpenAI budget limit

### "Workflow failed"
**Check:**
- GitHub Action logs for error messages
- OpenAI API key is valid and has credits
- No rate limiting errors
- Network connectivity

## Summary

**When does AI run?**
- Daily at 6am UTC (automatic)
- On-demand via GitHub Actions UI (manual)

**Do you need to run it again?**
- YES - Your current events don't have AI enrichment yet
- Trigger manually to enrich existing events

**Will it throttle the API?**
- NO - Smart caching prevents excessive API calls
- First run: ~$0.001 per event
- Subsequent runs: Mostly cached (free)
- Set OpenAI budget limit for safety

**Next steps:**
1. Trigger workflow manually in GitHub Actions
2. Watch logs to verify it works
3. Check exports/events.csv for enriched descriptions
4. Set OpenAI budget limit ($10-20/month)
5. Let it run daily automatically
