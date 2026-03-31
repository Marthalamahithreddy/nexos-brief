import urllib.request, urllib.parse, json, re, os, time
from datetime import datetime

TOKEN = os.getenv("TELEGRAM_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
DATA_DIR = '/data/nexos'

# ================================================================
# VARIABLE FLOW — output of each phase = input of next
#
# PHASE 1 output -> raw_posts (list of raw dicts, no filtering)
#                   seen (set loaded from disk)
#
# PHASE 2 input  -> raw_posts, seen
# PHASE 2 output -> posts (coded dicts with all enriched fields)
#   each dict: {title, url, sub, score, body, num_comments,
#               flair, awards, crosspost_from, is_crosspost,
#               top_comments: [{body, score, author}],
#               top_commenter, comment_sentiment,
#               summary, kw}
#
# PHASE 3 input  -> posts
# PHASE 3 output -> clusters (dict: tag -> [post dicts])
#
# PHASE 4 input  -> clusters
# PHASE 4 output -> final_clusters (pruned, merged, ordered)
#
# PHASE 5 input  -> final_clusters
# PHASE 5 output -> named_clusters (name, essence, posts)
#
# PHASE 6 input  -> named_clusters, posts
# PHASE 6 output -> Telegram message string (sent + printed)
#
# DISK STATE:
#   /data/nexos/reddit_seen.json  - titles seen (last 500)
#   Updated end of Phase 2 only
# ================================================================

def tg(msg):
    try:
        p = urllib.parse.urlencode({'chat_id': CHAT_ID, 'text': msg}).encode()
        r = urllib.request.Request('https://api.telegram.org/bot' + TOKEN + '/sendMessage', data=p)
        resp = json.loads(urllib.request.urlopen(r, timeout=10).read())
        print('[TG]', 'OK' if resp.get('ok') else 'FAIL: ' + resp.get('description','?'))
    except Exception as e:
        print('[TG ERR]', e)

def fetch(url):
    try:
        r = urllib.request.Request(url, headers={'User-Agent': 'NexosBrief/1.0'})
        return urllib.request.urlopen(r, timeout=15).read().decode('utf-8', 'ignore')
    except Exception as e:
        print('[FETCH ERR]', url[:60], e)
        return ''

def clean(t):
    if not t: return ''
    t = re.sub(r'<[^>]+>', '', str(t))
    for a,b in [('&amp;','and'),('&','and'),('&#39;',chr(39)),('&quot;',chr(34)),('&lt;','<'),('&gt;','>')]:
        t = t.replace(a,b)
    return t.encode('ascii','ignore').decode().strip()

def load(path, d):
    try: return json.loads(open(path).read())
    except: return d

def save(path, data):
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        open(path,'w').write(json.dumps(data, indent=2))
    except Exception as e: print('[SAVE ERR]', e)

def fetch_comments(permalink, limit=8):
    # Fetches top comments for a post using public .json API
    # Returns list of {body, score, author} sorted by score
    url = 'https://www.reddit.com' + permalink + '.json?limit=' + str(limit) + '&sort=top'
    raw = fetch(url)
    if not raw: return []
    comments = []
    try:
        data = json.loads(raw)
        # data[1] is the comments listing
        if len(data) < 2: return []
        for child in data[1]['data']['children']:
            d = child.get('data', {})
            kind = child.get('kind','')
            if kind != 't1': continue  # t1 = comment
            body = clean(d.get('body',''))
            score = d.get('score', 0)
            author = d.get('author','')
            if body and body not in ['[deleted]','[removed]'] and score > 0:
                comments.append({'body': body[:200], 'score': score, 'author': author})
    except Exception as e:
        print('[COMMENT ERR]', e)
    return sorted(comments, key=lambda x: -x['score'])[:5]

def award_count(post_data):
    # Returns total award count from post data
    return post_data.get('total_awards_received', 0)

def flair_text(post_data):
    # Returns cleaned flair text if exists
    f = post_data.get('link_flair_text') or post_data.get('link_flair_richtext')
    if isinstance(f, list):
        f = ' '.join(x.get('t','') for x in f if isinstance(x, dict))
    return clean(str(f)) if f else ''

# ================================================================
# PHASE 1: FAMILIARIZATION
# B&C: Immerse in ALL raw data before any filtering or coding.
# What happens:
#   - Fetch top 50 posts from 5 startup subreddits (top/day)
#   - Extract ALL available fields: title, body, score,
#     num_comments, flair, awards, crosspost info, permalink
#   - Load disk cache of seen post titles
#   - NO filtering yet — everything goes into raw_posts
# Output:
#   raw_posts = list of raw dicts (50 items max)
#   seen      = set of previously processed titles
# ================================================================
print('')
print('PHASE 1: FAMILIARIZATION')
print('Fetching all raw posts + metadata, no filtering yet')

CACHE = DATA_DIR + '/reddit_seen.json'
seen = set(load(CACHE, []))
print('  Cache: %d previously seen titles loaded' % len(seen))

SUBS = 'IndiaStartups+IndiaInvestments+startups+indianstartup+IndiaBusiness'
raw = fetch('https://www.reddit.com/r/' + SUBS + '/top.json?limit=50&t=day')
raw_posts = []
if raw:
    try:
        for child in json.loads(raw)['data']['children']:
            d = child['data']
            # Crosspost detection
            is_xpost = bool(d.get('crosspost_parent_list'))
            xpost_from = ''
            if is_xpost and d.get('crosspost_parent_list'):
                orig = d['crosspost_parent_list'][0]
                xpost_from = 'r/' + orig.get('subreddit','')

            raw_posts.append({
                'title':        clean(d.get('title','')),
                'url':          'https://reddit.com' + d.get('permalink',''),
                'permalink':    d.get('permalink',''),
                'sub':          d.get('subreddit',''),
                'score':        d.get('score', 0),
                'body':         clean(d.get('selftext',''))[:400],
                'num_comments': d.get('num_comments', 0),
                'flair':        flair_text(d),
                'awards':       award_count(d),
                'is_crosspost': is_xpost,
                'xpost_from':   xpost_from,
            })
    except Exception as e:
        print('[ERR] Reddit parse:', e)

print('  Fetched %d raw posts' % len(raw_posts))
print('  Fields per post: title, url, sub, score, body, num_comments,')
print('                   flair, awards, is_crosspost, xpost_from')
print('Phase 1 done. raw_posts -> Phase 2')

# ================================================================
# PHASE 2: GENERATING INITIAL CODES
# B&C: Systematically tag every data point with codes.
# What happens:
#   - Filter: skip seen posts, skip score < 5
#   - For each new post: FETCH COMMENT THREAD (top 5 comments)
#   - Extract: top_comments, top_commenter, comment_sentiment
#   - Build summary using title + body + top comment signal
#   - Assign kw tags from TAXONOMY (multi-tag allowed)
#   - Fund tag only if explicit crore/million/series language
#   - Each coded post stored as full dict
# Output:
#   posts = list of coded dicts with all fields
#   (also saves new seen titles to disk)
# ================================================================
print('')
print('PHASE 2: GENERATING INITIAL CODES')
print('For each new post: fetch comments, tag, summarize')

TAXONOMY = {
    'fundraising':  ['raised funding','raised seed','raised series','raised crore','raised rs','raised dollar','funding round','crore raised','million raised','series a','series b','series c','pre-seed round','angel round','angel investment','vc funding','secures funding','bags funding','fundraising','fundraise','raised capital'],
    'growth':       ['growth','scale','revenue','arr','mrr','profit','traction','milestone','customers','expand','user base'],
    'hiring':       ['hiring','hire','job','jobs','team','recruit','talent','engineer','developer','remote work'],
    'product':      ['product','launch','launched','feature','saas','b2b','api','platform','tool','app','software','build'],
    'founder':      ['founder','co-founder','founding','startup','bootstrapp','solo','idea','journey','story','lesson'],
    'india':        ['india','indian','bangalore','mumbai','delhi','pune','hyderabad','chennai','bengaluru','bharat'],
    'strategy':     ['strategy','customer','sales','gtm','retention','churn','pricing','model','acquisition'],
    'ai':           ['ai','llm','gpt','ml','artificial intelligence','machine learning','automation','agent'],
    'problem':      ['problem','struggle','challenge','issue','fail','mistake','advice','help','how to','what should'],
    'acquisition':  ['acqui','acquired','acquires','merger','exit','ipo','buyout'],
    'regulation':   ['sebi','rbi','government','regulation','law','tax','compliance','policy','legal'],
    'market':       ['market','industry','sector','ecosystem','trend','report','data','research','analysis'],
}

def build_summary(title, body, top_comments, kw_list, flair, awards, xpost_from):
    # Build a specific 1-3 line code from all available signals
    t = title.lower()
    parts = []

    # Line 1: categorized title
    real_fund = any(w in t or w in body.lower() for w in ['crore','million',' seed ',' series ','fundrais','raised capital','vc fund','angel invest'])
    if 'fundraising' in kw_list and real_fund:
        parts.append('Capital event: ' + title[:80])
    elif 'problem' in kw_list or 'strategy' in kw_list:
        parts.append('Founder Q&A: ' + title[:80])
    elif 'product' in kw_list:
        parts.append('Product signal: ' + title[:80])
    elif 'hiring' in kw_list:
        parts.append('Hiring signal: ' + title[:80])
    elif 'ai' in kw_list:
        parts.append('AI/Tech: ' + title[:80])
    elif 'acquisition' in kw_list:
        parts.append('M&A: ' + title[:80])
    elif 'founder' in kw_list:
        parts.append('Founder: ' + title[:80])
    elif 'regulation' in kw_list:
        parts.append('Regulatory: ' + title[:80])
    else:
        parts.append(title[:80])

    # Line 2: metadata signals (flair, awards, crosspost)
    meta = []
    if flair: meta.append('flair:' + flair[:25])
    if awards > 0: meta.append('%d award%s' % (awards, 's' if awards>1 else ''))
    if xpost_from: meta.append('xpost from ' + xpost_from)
    if meta: parts.append('  [' + ' | '.join(meta) + ']')

    # Line 3: best comment signal (highest scored comment)
    if top_comments:
        best = top_comments[0]
        cb = best['body'].strip()
        if cb and len(cb) > 15:
            parts.append('  top comment (' + str(best['score']) + 'pts): ' + cb[:120])

    return chr(10).join(parts)

def comment_sentiment(comments):
    # Simple sentiment from comment text
    pos_w = ['great','good','yes','agree','exactly','true','helpful','thanks','nice','correct','love','awesome','brilliant','solid','works','success']
    neg_w = ['bad','wrong','no','disagree','terrible','fail','issue','problem','broken','scam','avoid','warning','careful','worst','never','regret']
    score = 0
    for c in comments:
        t = c['body'].lower()
        score += sum(1 for w in pos_w if w in t)
        score -= sum(1 for w in neg_w if w in t)
    if score > 1: return 'positive'
    if score < -1: return 'negative'
    return 'mixed'

posts = []
new_seen = list(seen)
for i, rp in enumerate(raw_posts):
    title = rp['title']
    if not title or rp['score'] < 5: continue
    if title in seen:
        print('  [skip-seen] ' + title[:55])
        continue

    # Fetch comment thread for this post
    print('  [fetching comments %d/%d] %s' % (i+1, len(raw_posts), title[:45]))
    comments = fetch_comments(rp['permalink'], limit=8)
    time.sleep(0.5)  # polite rate limit

    # Assign keyword tags
    combined = (title + ' ' + rp['body'] + ' ' + ' '.join(c['body'] for c in comments)).lower()
    kw = [tag for tag, triggers in TAXONOMY.items() if any(w in combined for w in triggers)]

    # Drop if no signal and low score
    if not kw and rp['score'] < 10:
        print('  [skip-nokw] score=%d | %s' % (rp['score'], title[:45]))
        continue

    top_commenter = comments[0]['author'] if comments else ''
    csent = comment_sentiment(comments)
    summary = build_summary(title, rp['body'], comments, kw, rp['flair'], rp['awards'], rp['xpost_from'])

    coded = {
        'title':         title,
        'url':           rp['url'],
        'sub':           rp['sub'],
        'score':         rp['score'],
        'body':          rp['body'],
        'num_comments':  rp['num_comments'],
        'flair':         rp['flair'],
        'awards':        rp['awards'],
        'is_crosspost':  rp['is_crosspost'],
        'xpost_from':    rp['xpost_from'],
        'top_comments':  comments,
        'top_commenter': top_commenter,
        'comment_sentiment': csent,
        'summary':       summary,
        'kw':            kw,
    }
    posts.append(coded)
    new_seen.append(title)
    print('  [coded] score=%d cmts=%d kw=%s csent=%s | %s' % (
        rp['score'], len(comments), kw, csent, title[:45]))

save(CACHE, new_seen[-500:])
print('Phase 2 done. %d posts coded -> Phase 3' % len(posts))

# ================================================================
# PHASE 3: GENERATING INITIAL THEMES
# B&C: Group coded items into emergent clusters by shared kw.
# What happens:
#   - For each tag in TAXONOMY, collect all posts that have it
#   - Cluster only if >= 2 posts share that tag (theme threshold)
#   - Posts can belong to multiple clusters (multi-kw)
#   - Cluster key = tag name (renamed in Phase 5)
# Output:
#   clusters = dict {tag -> [post dicts]}
# ================================================================
print('')
print('PHASE 3: GENERATING INITIAL THEMES')
print('Clustering by shared kw tags (emergent, not preset)')

clusters = {}
for tag in TAXONOMY.keys():
    matched = [p for p in posts if tag in p['kw']]
    if len(matched) >= 2:
        clusters[tag] = matched
        print('  Cluster %-14s: %d posts' % (tag, len(matched)))
    elif len(matched) == 1:
        print('  (singleton) %-14s: only 1 post, not a theme' % tag)

print('Phase 3 done. %d clusters -> Phase 4' % len(clusters))

# ================================================================
# PHASE 4: REVIEWING THEMES
# B&C: Test each cluster. Discard weak. Merge if overlapping.
# What happens:
#   - Sort clusters largest -> smallest
#   - If cluster X is >70% subset of already-kept cluster Y,
#     discard X (absorbed into Y)
#   - Orphan posts (in no cluster) -> misc if >= 2
# Output:
#   final_clusters = sorted list of (tag, [posts]) by size
# ================================================================
print('')
print('PHASE 4: REVIEWING THEMES')
print('Absorbing subsets, collecting orphans')

sorted_c = sorted(clusters.items(), key=lambda x: -len(x[1]))
kept = {}
for tag, tag_posts in sorted_c:
    titles = set(p['title'] for p in tag_posts)
    absorbed = False
    for kt, kp in kept.items():
        kt_titles = set(p['title'] for p in kp)
        overlap = len(titles & kt_titles)
        if overlap / len(titles) > 0.70:
            print('  [absorb] %s -> %s (%.0f%% overlap)' % (tag, kt, 100*overlap/len(titles)))
            absorbed = True; break
    if not absorbed:
        kept[tag] = tag_posts
        print('  [keep]   %-14s: %d posts' % (tag, len(tag_posts)))

all_ct = set(p['title'] for t,ps in kept.items() for p in ps)
orphans = [p for p in posts if p['title'] not in all_ct]
if len(orphans) >= 2:
    kept['misc'] = orphans
    print('  [keep]   misc          : %d posts (unclustered)' % len(orphans))

final_clusters = sorted(kept.items(), key=lambda x: -len(x[1]))
print('Phase 4 done. %d themes -> Phase 5' % len(final_clusters))

# ================================================================
# PHASE 5: DEFINING AND NAMING THEMES
# B&C: Finalise each cluster's name and essence statement.
# What happens:
#   - TAG_NAMES maps tag -> (display name, essence)
#   - named_clusters = list of (name, essence, posts)
# Output:
#   named_clusters = list of (name, essence, [post dicts])
# ================================================================
print('')
print('PHASE 5: DEFINING AND NAMING THEMES')

TAG_NAMES = {
    'fundraising':  ('FUNDING SIGNALS',   'Capital activity in the ecosystem'),
    'growth':       ('GROWTH & SCALE',    'Revenue, traction, expansion talk'),
    'hiring':       ('HIRING SIGNALS',    'Talent movement and team building'),
    'product':      ('PRODUCT & LAUNCHES','New products, features, tools'),
    'founder':      ('FOUNDER TALK',      'Journeys, insights, decisions'),
    'india':        ('INDIA ECOSYSTEM',   'India-specific startup signals'),
    'strategy':     ('STRATEGY & GTM',    'Sales, pricing, go-to-market'),
    'ai':           ('AI & TECH',         'AI/ML product and discussion'),
    'problem':      ('COMMUNITY Q&A',     'Founders asking for help'),
    'acquisition':  ('M&A / EXITS',       'Acquisitions, mergers, IPOs'),
    'regulation':   ('REGULATORY',        'Policy, compliance, government'),
    'market':       ('MARKET INTEL',      'Trends, data, industry analysis'),
    'misc':         ('OTHER SIGNALS',     'Uncategorized startup discussion'),
}

named_clusters = []
for tag, tag_posts in final_clusters:
    name, essence = TAG_NAMES.get(tag, (tag.upper(), 'Startup discussion'))
    named_clusters.append((name, essence, tag_posts))
    print('  %s -> %s | %d posts' % (tag, name, len(tag_posts)))

print('Phase 5 done. %d named themes -> Phase 6' % len(named_clusters))

# ================================================================
# PHASE 6: WRITING UP
# B&C: Produce the final report with vivid data extracts.
# What happens:
#   - Top 4 clusters by post count
#   - Per post: show summary + top comment + signals
#   - Awards, flair, crosspost shown if present
#   - Comment sentiment shown per post
#   - Clean output: no phase labels, no internal vars
#   - Telegram 4096 char limit enforced
# Output:
#   msg = final string, sent to Telegram
# ================================================================
print('')
print('PHASE 6: WRITING UP')

now = datetime.now()
L = []
L.append('NEXOS BRIEF  ' + now.strftime('%a %d %b') + '  ' + now.strftime('%I:%M %p') + ' IST')
L.append('[posts: %d | themes: %d]' % (len(posts), len(named_clusters)))

if not named_clusters:
    L.append(''); L.append('No new startup discussion today.')
else:
    for name, essence, theme_posts in named_clusters[:4]:
        L.append('')
        L.append(name)
        L.append(essence)
        top = sorted(theme_posts, key=lambda x: -(x['score'] + x['num_comments']))[:3]
        for p in top:
            L.append('')
            # Summary lines
            for line in p['summary'].split(chr(10)):
                if line.strip():
                    L.append(line.strip())
            # Engagement signals
            sigs = []
            if p['score'] > 0: sigs.append('%dup' % p['score'])
            if p['num_comments'] > 0: sigs.append('%dcmt' % p['num_comments'])
            if p['comment_sentiment'] != 'mixed': sigs.append(p['comment_sentiment'])
            if sigs: L.append('  [' + ' | '.join(sigs) + ']')
            # Link
            L.append('  ' + p['url'])

msg = chr(10).join(L)
if len(msg) > 4000:
    msg = msg[:3950] + chr(10) + '[trimmed]'

print('')
print('=== TELEGRAM OUTPUT ===')
print(msg)
print('=== END ===')
print('chars:', len(msg))
tg(msg)
print('DONE')