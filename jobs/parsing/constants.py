
import re

COMPANY_NOISE_TOKENS = {'danmark', 'denmark', 'aps', 'a', 's', 'as', 'ab', 'oy', 'ltd', 'llc', 'inc', 'group', 'holding'}
LEARNING_STOPWORDS = {'about', 'above', 'after', 'again', 'against', 'all', 'also', 'and', 'any', 'are', 'because', 'been', 'before', 'being', 'below', 'between', 'both', 'but', 'can', 'company', 'could', 'danish', 'denmark', 'developer', 'email', 'for', 'from', 'have', 'into', 'job', 'jobs', 'just', 'more', 'not', 'our', 'out', 'position', 'role', 'than', 'that', 'the', 'their', 'them', 'there', 'these', 'this', 'those', 'through', 'under', 'using', 'very', 'want', 'when', 'where', 'which', 'with', 'you', 'your'}
EASY_APPLY_PATTERN = re.compile(r'\beasy\s*apply\b', flags=re.IGNORECASE)
