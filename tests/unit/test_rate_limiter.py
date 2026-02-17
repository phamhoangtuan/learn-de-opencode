"""T023: Unit test — rate_limiter token bucket algorithm.

Per Constitution v2.0.0 Principle III: Tests written FIRST, must FAIL before implementation.
Per spec.md FR-002: Configurable rate from 1 to 1000 tx/sec.
"""

import time

import pytest


class TestTokenBucketRateLimiter:
    """Test token bucket rate limiter for transaction generation."""

    def test_initial_bucket_allows_one_token(self) -> None:
        """A new rate limiter should allow at least one immediate acquire."""
        from generator.src.lib.rate_limiter import RateLimiter

        limiter = RateLimiter(rate=10)
        assert limiter.acquire() is True

    def test_rate_limiter_enforces_rate(self) -> None:
        """Rate limiter must not allow more than rate tokens per second."""
        from generator.src.lib.rate_limiter import RateLimiter

        rate = 100
        limiter = RateLimiter(rate=rate)

        acquired = 0
        start = time.monotonic()
        # Try to acquire as many as possible in 0.1 seconds
        while time.monotonic() - start < 0.1:
            if limiter.acquire():
                acquired += 1

        # In 0.1s at rate=100, we should get ~10 tokens (+ initial burst)
        # Allow generous upper bound due to initial bucket
        assert acquired <= rate * 0.2 + rate, (
            f"Acquired {acquired} in 0.1s at rate={rate}, too many"
        )

    def test_rate_limiter_refills_over_time(self) -> None:
        """Tokens should refill over time at the configured rate."""
        from generator.src.lib.rate_limiter import RateLimiter

        limiter = RateLimiter(rate=100)
        # Drain initial tokens
        while limiter.acquire():
            pass

        # Wait for refill
        time.sleep(0.05)  # Should refill ~5 tokens at rate=100
        assert limiter.acquire() is True, "Tokens should refill over time"

    def test_rate_limiter_acquire_blocks_false(self) -> None:
        """acquire() should return False when no tokens available (non-blocking)."""
        from generator.src.lib.rate_limiter import RateLimiter

        limiter = RateLimiter(rate=1)
        limiter.acquire()  # Use the initial token

        # Immediately try again — should fail
        result = limiter.acquire()
        assert result is False, "Should return False when no tokens available"

    def test_rate_limiter_wait_blocks_until_available(self) -> None:
        """wait() should block until a token is available."""
        from generator.src.lib.rate_limiter import RateLimiter

        limiter = RateLimiter(rate=10)
        # Drain all tokens
        while limiter.acquire():
            pass

        start = time.monotonic()
        limiter.wait()  # Should block until a token refills
        elapsed = time.monotonic() - start

        # At rate=10, refill interval is 0.1s
        assert elapsed >= 0.05, f"wait() returned too quickly: {elapsed:.3f}s"
        assert elapsed < 1.0, f"wait() took too long: {elapsed:.3f}s"

    def test_update_rate_changes_token_generation(self) -> None:
        """update_rate() must change the refill rate without reset."""
        from generator.src.lib.rate_limiter import RateLimiter

        limiter = RateLimiter(rate=10)
        limiter.update_rate(1000)
        assert limiter.rate == 1000

    def test_rate_must_be_positive(self) -> None:
        """Rate must be at least 1 per spec.md FR-002."""
        from generator.src.lib.rate_limiter import RateLimiter

        with pytest.raises(ValueError, match="[Rr]ate"):
            RateLimiter(rate=0)

        with pytest.raises(ValueError, match="[Rr]ate"):
            RateLimiter(rate=-5)

    def test_burst_handling(self) -> None:
        """Rate limiter should handle burst by allowing up to rate tokens initially."""
        from generator.src.lib.rate_limiter import RateLimiter

        rate = 10
        limiter = RateLimiter(rate=rate)

        # Should be able to acquire initial burst
        burst_count = 0
        while limiter.acquire():
            burst_count += 1
            if burst_count > rate * 2:
                break

        assert burst_count >= 1, "Should allow at least 1 token initially"
        assert burst_count <= rate * 2, f"Burst {burst_count} too large for rate {rate}"
