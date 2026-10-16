"""Fix all bare 'return sf_query(' patterns in analytics.py to use _cast_numerics."""
from pathlib import Path

p   = Path("dashboard/pages/analytics.py")
src = p.read_text(encoding="utf-8")

# Replace every: return sf_query("""...""")
# with:          df = sf_query("""...""")\n    return _cast_numerics(df)
# BUT only for loaders (they end with triple-quote + closing paren + newline)
import re

# Match the pattern inside @cache_data functions
pattern = r'(    )return (sf_query\(f?""".*?"""\))'
replacement = r'\1df = \2\n\1return _cast_numerics(df)'

new_src = re.sub(pattern, replacement, src, flags=re.DOTALL)

# Verify no bare return sf_query remains
remaining = re.findall(r'return sf_query\(', new_src)
print(f"Remaining bare 'return sf_query(' occurrences: {len(remaining)}")
if remaining:
    print("  Still found:", remaining)

p.write_text(new_src, encoding="utf-8")
print("Done.")
