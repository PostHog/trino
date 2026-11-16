# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Generates the ClickHouse multiMatchAnyIndex fixtures for TestClickHouseRegexFunctions.

    docker run -d --name clickhouse -p 8123:8123 -e CLICKHOUSE_SKIP_USER_SETUP=1 clickhouse/clickhouse-server:26.10
    python3 generate-multimatchanyindex-fixtures.py http://localhost:8123/ bot_definitions.py .

bot_definitions.py is products/web_analytics/backend/hogql_queries/bot_definitions.py from
PostHog/posthog. Only the keys of its BOT_DEFINITIONS dict literal are read, with ast, so the
file is never imported. The fixtures were generated with ClickHouse 26.10.3.7 from PostHog/posthog
blob d92137bce4b1d9316cb22d1b4779a09a7e75759e, and ClickHouse 25.8.33.6 returns the same results.
"""

import ast
import json
import random
import sys
import urllib.error
import urllib.request

# Synthetic user agents: browsers, crawlers, AI agents, HTTP libraries and edge cases that
# exercise the regular expressions among the bot patterns
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.1 Safari/605.1.15",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 18_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.1 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (X11; Linux x86_64; rv:133.0) Gecko/20100101 Firefox/133.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0",
    "Mozilla/5.0 (Linux; Android 14; Pixel 8) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.6778.135 Mobile Safari/537.36",
    "Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Mobile Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edge/16.16299",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edge/19.0",
    "Mozilla/5.0 (Linux; Android 13) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.6422.1234 Mobile Safari/537.36",
    "Mozilla/5.0 (Linux; Android 13) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.6422.1234 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) HeadlessChrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.3",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0",
    " Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) ",
    "Mozilla/5.0\t",
    "",
    "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)",
    "Mozilla/5.0 (Linux; Android 6.0.1; Nexus 5X Build/MMB29P) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.6778.204 Mobile Safari/537.36 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)",
    "Mozilla/5.0 (compatible; bingbot/2.0; +http://www.bing.com/bingbot.htm)",
    "Mozilla/5.0 AppleWebKit/537.36 (KHTML, like Gecko; compatible; GPTBot/1.2; +https://openai.com/gptbot)",
    "Mozilla/5.0 AppleWebKit/537.36 (KHTML, like Gecko; compatible; ClaudeBot/1.0; +claudebot@anthropic.com)",
    "Mozilla/5.0 AppleWebKit/537.36 (KHTML, like Gecko; compatible; ChatGPT-User/1.0; +https://openai.com/bot)",
    "Mozilla/5.0 (compatible; PerplexityBot/1.0; +https://perplexity.ai/perplexitybot) Googlebot",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Claude/0.14.10 Chrome/130.0.6723.152 Electron/33.2.1 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) ChatGPT/1.2025.014 Chrome/130.0.6723.191 Electron/33.3.1 Safari/537.36",
    "opencode/0.3.110",
    "opencode-cli 1.0",
    "Cursor/0.48.6 (darwin arm64)",
    "Cursor/0.48.6",
    "claude-code/1.0.33 (external, cli)",
    "MiniMaxAgent/1.0",
    "node",
    "node-fetch/1.0",
    "n8n",
    "Ruby",
    "Java/17.0.2",
    "colly - https://github.com/gocolly/colly",
    "got (https://github.com/sindresorhus/got)",
    "curl/8.5.0",
    "Wget/1.21.4",
    "python-requests/2.32.3",
    "axios/1.7.9",
    "Go-http-client/2.0",
    "PostHog/1.0 (desktop.hog.dev)",
    "Mozilla/5.0 (compatible; archive.org_bot +http://archive.org/details/archive.org_bot)",
    "Mozilla/5.0 (compatible; seo4ajax.com crawler)",
    "Mozilla/5.0 (compatible; seo4ajaxXcom)",
    "linkchecker.pro/1.0",
    "Synapse (bot; +https://github.com/matrix-org/synapse)",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Ünïcødé ✓ 🎉",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36\n",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) ",
    "AhrefsBot SemrushBot curl/8.5.0 Googlebot",
]

FUZZ_CHARS = ['A', 'aa', '\t', '\x0b', ' ', ' ', '\u0085', '\x01', '\x1b', '{', '}', ']', '\x08', 'a.b',
              'İ', 'ı', 'I', 'i', 'a', 'b', 'c', ' ', '\n', 'é', 'K', 'k', 'B', '1', '_', '€',
              '\U0001f600', 'x', '.', '-', 'S', 's', 'ſ', 'K']
FUZZ_LITERALS = ['\\141', '\\0141', '\\x61', '\\x{20ac}', '\\Qa.b\\E', 'a{,2}', 'x{', '}', ']', '\\cA', '\\e', '\\t',
                 '\\y', '\\8', '\\11', '\\012', '\\o{142}', 'a', 'b', 'c', ' ', '\\n', 'é', 'K', 'k', 'B', '1',
                 '_', '€', '\U0001f600', 'x', '\\.', '-', '\\x{e9}', 'ab', 'bc', 'S', 'ſ']
# Only negated classes that exclude a single range: ClickHouse 26.10 matches the others incorrectly
FUZZ_CLASSES = ['[[:digit:]_]', '[[:^digit:]]', '\\p{L}', '\\pN', '[\\p{Lu}a]', '\\p{Latin}', '[\\x{e9}-\\x{20ac}]',
                '[]a]', '[a-]', '[\\d-z]', '[\\Q]\\E]', '[\\b]', '[\\141]', '\\v', '[^ab]', '\\p{Xan}', '\\p{Any}',
                '[[:punct:]]', '.', '[abc]', '[^a]', '[a-c]', '[^\\n]', '\\d', '\\w', '\\s', '\\D', '[[:alpha:]]',
                '[é€]', '[^é]', '\\h', '[\\w.]']
NEGATIONS = ['[^', '\\D', '[:^', '[[:^']


def query(url, sql):
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    request = urllib.request.Request(url, data=sql.encode(), method="POST")
    try:
        with opener.open(request, timeout=60) as response:
            return response.read().decode()
    except urllib.error.HTTPError as e:
        return None


def sql_string_array(values):
    if not values:
        return "CAST([] AS Array(String))"
    return "[" + ",".join(f"unhex('{value.encode().hex()}')" for value in values) + "]"


def multi_match_any_index(url, haystacks, patterns):
    result = query(url, f"SELECT arrayStringConcat(arrayMap(h -> toString(multiMatchAnyIndex(h, {sql_string_array(patterns)})), "
                        f"{sql_string_array(haystacks)}), ',') FORMAT TSVRaw")
    if result is None:
        return None
    return [int(value) for value in result.strip().split(",")]


def bot_patterns(path):
    tree = ast.parse(open(path, encoding="utf-8").read())
    for node in tree.body:
        if isinstance(node, ast.AnnAssign) and getattr(node.target, "id", None) == "BOT_DEFINITIONS":
            return [key.value for key in node.value.keys]
    raise ValueError("BOT_DEFINITIONS not found")


def fuzz_atom(depth):
    r = random.random()
    if r < 0.45:
        return random.choice(FUZZ_LITERALS)
    if r < 0.75:
        return random.choice(FUZZ_CLASSES)
    if r < 0.85 and depth < 2:
        return '(' + fuzz_alternation(depth + 1) + ')'
    if r < 0.92 and depth < 2:
        return '(?:' + fuzz_alternation(depth + 1) + ')'
    return random.choice(FUZZ_LITERALS)


def fuzz_quantifier():
    if random.random() < 0.65:
        return ''
    quantifier = random.choice(['*', '+', '?', '{2}', '{1,3}', '{0,2}', '{2,}'])
    if random.random() < 0.2:
        quantifier += '?'
    return quantifier


def fuzz_sequence(depth):
    parts = []
    if random.random() < 0.12:
        parts.append(random.choice(['^', '\\A']))
    for _ in range(random.randint(1, 4)):
        if random.random() < 0.12:
            parts.append(random.choice(['\\b', '\\B']))
        parts.append(fuzz_atom(depth) + fuzz_quantifier())
    if random.random() < 0.12:
        parts.append(random.choice(['\\b', '\\B']))
    if random.random() < 0.15:
        parts.append(random.choice(['$', '\\z', '\\Z']))
    return ''.join(parts)


def fuzz_alternation(depth):
    count = 1 if random.random() < 0.7 else random.randint(2, 3)
    return '|'.join(fuzz_sequence(depth) for _ in range(count))


def fuzz_pattern():
    pattern = fuzz_alternation(0)
    r = random.random()
    caseless = None
    if r < 0.03 and '[' not in pattern:
        pattern = '(?x)' + pattern.replace('a', ' a ') + ' # comment\n'
    elif r < 0.05:
        pattern = pattern + '(?#comment)'
    elif r < 0.07:
        caseless = '(?i:' + pattern + ')b'
    elif r < 0.08:
        caseless = 'a(?i)' + pattern
    elif r < 0.2:
        caseless = '(?i)' + pattern
    elif r < 0.26:
        pattern = '(?m)' + pattern
    elif r < 0.29:
        pattern = '(?-s)' + pattern
    # Hyperscan folds negated classes inconsistently in case-insensitive mode
    if caseless is not None and not any(negation in pattern for negation in NEGATIONS):
        pattern = caseless
    return pattern


def fuzz_haystack():
    return ''.join(random.choice(FUZZ_CHARS) for _ in range(random.randint(0, 12)))


def main():
    url, definitions, output = sys.argv[1], sys.argv[2], sys.argv[3]
    print(query(url, "SELECT version()").strip(), file=sys.stderr)

    patterns = bot_patterns(definitions)
    with open(f"{output}/multimatchanyindex-bot-patterns.json", "w", encoding="utf-8") as file:
        json.dump(patterns, file, ensure_ascii=False, indent=0)
        file.write("\n")
    # posthog/hogql/functions/traffic_type.py calls multiMatchAnyIndex with the patterns plus '^$',
    # and without it when a project has its own rules
    with_empty = multi_match_any_index(url, USER_AGENTS, patterns + ["^$"])
    without_empty = multi_match_any_index(url, USER_AGENTS, patterns)
    with open(f"{output}/multimatchanyindex-bots-clickhouse.tsv", "w", encoding="utf-8") as file:
        for user_agent, first, second in zip(USER_AGENTS, with_empty, without_empty):
            file.write(f"{user_agent.encode().hex()}\t{first}\t{second}\n")

    random.seed(20261113)
    with open(f"{output}/multimatchanyindex-fuzz-clickhouse.jsonl", "w", encoding="utf-8") as file:
        written = 0
        while written < 400:
            patterns = [fuzz_pattern() for _ in range(random.randint(1, 5))]
            haystacks = [fuzz_haystack() for _ in range(8)]
            results = multi_match_any_index(url, haystacks, patterns)
            if results is None:
                # ClickHouse rejected a pattern, for example one that can never match
                continue
            file.write(json.dumps({
                "patterns": [pattern.encode().hex() for pattern in patterns],
                "haystacks": [haystack.encode().hex() for haystack in haystacks],
                "results": results}) + "\n")
            written += 1


main()
