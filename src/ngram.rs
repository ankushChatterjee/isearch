// Weight: deterministic hash of a character pair.
// Higher weight = rarer pair = preferred n-gram boundary.
// Start with CRC32. Later: swap for a frequency table built from real code.
pub fn pair_weight(a: u8, b: u8) -> u32 {
    crc32fast::hash(&[a, b])
}

// INDEX TIME: extract every valid sparse n-gram from a document.
// A substring s[i..=j] is valid when weight[i] and weight[j-1]
// are both strictly greater than every interior weight.
//
// Duplicates are NOT removed here. Callers that need unique hashes per
// document (e.g. Index::build) dedup at the (hash, doc_id) level after
// hashing, which is cheaper than maintaining a HashSet<&[u8]> per file.
pub fn extract_all_ngrams(text: &[u8]) -> impl Iterator<Item = &[u8]> {
    let n = text.len();
    let weights: Vec<u32> = (0..n.saturating_sub(1))
        .map(|i| pair_weight(text[i], text[i + 1]))
        .collect();

    let mut out = vec![];

    for i in 0..n.saturating_sub(1) {
        let w_start = weights[i];
        let mut max_interior = 0u32;

        for j in (i + 1)..n {
            // Right boundary is the pair *ending* at j: weights[j-1] = pair(text[j-1], text[j]).
            let w_end = weights[j - 1];

            // Bigrams (j == i+1) and trigrams (j == i+2) are always valid: no interior pairs
            // exist, so "strictly greater than all interior weights" holds vacuously.
            // For longer spans, both boundary weights must exceed the MAX interior weight.
            let valid = j <= i + 2 || (w_start > max_interior && w_end > max_interior);

            if valid {
                out.push(&text[i..=j]);
            }

            // Grow the interior window: the right boundary pair of [i..=j] becomes an
            // interior pair when we extend to [i..=j+1], so add it to max_interior.
            if j >= i + 2 {
                max_interior = max_interior.max(weights[j - 1]);
                // Early exit: once max_interior ≥ w_start, the left boundary condition
                // (w_start > max_interior) can never hold again since max_interior
                // only grows — no larger span from i can be valid.
                if max_interior >= w_start {
                    break;
                }
            }
        }
    }
    out.into_iter()
}

// QUERY TIME: minimal set of n-grams that covers every byte position.
// Greedy: at each uncovered position, pick the longest valid n-gram.
pub fn covering_ngrams(text: &[u8]) -> Vec<&[u8]> {
    let n = text.len();
    if n < 2 {
        return vec![text];
    } // too short — just use the whole thing

    let weights: Vec<u32> = (0..n - 1)
        .map(|i| pair_weight(text[i], text[i + 1]))
        .collect();

    let mut pos = 0;
    let mut result = vec![];

    while pos < n {
        // Find the longest valid n-gram starting at pos
        let mut best_end = pos; // fallback: single char (uncovered edge case)
        let mut max_interior = 0u32;

        for j in (pos + 1)..n {
            let w_start = if pos < weights.len() { weights[pos] } else { 0 };
            let w_end = if j - 1 < weights.len() {
                weights[j - 1]
            } else {
                0
            };

            let valid = j <= pos + 2 || (w_start > max_interior && w_end > max_interior);

            if valid {
                best_end = j;
            }

            if j >= pos + 2 {
                max_interior = max_interior.max(weights[j - 1]);
                if max_interior >= w_start {
                    break;
                }
            }
        }

        result.push(&text[pos..=best_end]);
        pos = best_end + 1;
    }
    result
}

/// FNV-1a 32-bit over `ngram` bytes. Shorter keys than `u64` for the lookup table; collisions
pub fn hash_ngram(ngram: &[u8]) -> u32 {
    let mut h = 0x811c_9dc5u32;
    for &b in ngram {
        h ^= u32::from(b);
        h = h.wrapping_mul(0x0100_0193);
    }
    h
}
