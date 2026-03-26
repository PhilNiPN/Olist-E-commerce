"""
Tests for notification.py: the pipeline notification dispatch system.

PATTERN: We test each handler in isolation, then the dispatch loop (notify).
         No DB is needed — everything is controlled with unittest.mock.

KEY CONCEPTS:
  - patch.dict('os.environ', ...)          -> temporarily sets/clears environment variables
  - patch('requests.post')                 -> replaces the real HTTP call with a mock
  - patch('notification._HANDLERS', [...]) -> swaps the handler list with our mocks
  - MagicMock(side_effect=...)             -> makes the mock raise an exception when called
  - caplog                                 -> pytest fixture that captures log records
"""

import logging
from unittest.mock import patch, MagicMock

from notification import _log_handler, _slack_handler, notify, PipelineOutcome


def _make_outcome(**overrides):
    """
    Factory that builds a PipelineOutcome with sensible defaults.
    Pass keyword args to override any field, e.g. _make_outcome(status = 'success').
    This avoids repeating the full constructor in every test.
    """
    defaults = dict(
        run_id='run-1', layer='silver', status='failed',
        tables_loaded=5, tables_failed=1, dq_failures=[], tables_rejected=0,
    )
    defaults.update(overrides)
    return PipelineOutcome(**defaults)


### log handler

class TestLogHandler:
    """
    _log_handler should only emit a warning when something went wrong.
    On a clean 'success' run it should stay silent (no noise in logs).
    """

    def test_skips_on_success(self, caplog):
        # caplog captures log output; we assert nothing was recorded
        with caplog.at_level(logging.WARNING):
            _log_handler(_make_outcome(status='success'))
        assert caplog.records == []

    def test_logs_warning_on_failed(self, caplog):
        with caplog.at_level(logging.WARNING):
            _log_handler(_make_outcome(status='failed'))
        assert len(caplog.records) == 1
        assert caplog.records[0].levelno == logging.WARNING

    def test_logs_warning_on_success_with_warnings(self, caplog):
        # 'success_with_warnings' also deserves a log entry
        with caplog.at_level(logging.WARNING):
            _log_handler(_make_outcome(status='success_with_warnings'))
        assert len(caplog.records) == 1


### slack handler

class TestSlackHandler:
    """
    _slack_handler sends a Slack message via webhook.
    Three guard clauses to test:
      1. No SLACK_WEBHOOK_URL -> do nothing
      2. status == 'success' -> do nothing (even with webhook set)
      3. status != 'success' AND webhook set -> POST with correct emoji
    """

    def test_skips_when_no_webhook_url(self):
        # patch.dict with clear=True wipes all env vars for this block
        with patch.dict('os.environ', {}, clear=True):
            # Should return silently — no HTTP call, no error
            _slack_handler(_make_outcome(status='failed'))

    def test_skips_on_success(self):
        # Even with a webhook configured, 'success' should NOT trigger a message
        with patch.dict('os.environ', {'SLACK_WEBHOOK_URL': 'https://hooks.slack.com/test'}):
            with patch('requests.post') as mock_post:
                _slack_handler(_make_outcome(status='success'))
                mock_post.assert_not_called()

    def test_sends_post_with_rotating_light_on_failed(self):
        with patch.dict('os.environ', {'SLACK_WEBHOOK_URL': 'https://hooks.slack.com/test'}):
            # patch('requests.post') intercepts the real HTTP call
            with patch('requests.post') as mock_post:
                _slack_handler(_make_outcome(status='failed'))
                mock_post.assert_called_once()
                # Inspect the JSON body that was sent
                payload = mock_post.call_args
                assert ':rotating_light:' in payload.kwargs['json']['text']

    def test_sends_post_with_warning_emoji_on_warnings(self):
        with patch.dict('os.environ', {'SLACK_WEBHOOK_URL': 'https://hooks.slack.com/test'}):
            with patch('requests.post') as mock_post:
                _slack_handler(_make_outcome(status='success_with_warnings'))
                mock_post.assert_called_once()
                payload = mock_post.call_args
                # Different status -> different emoji
                assert ':warning:' in payload.kwargs['json']['text']


### notify dispatch

class TestNotify:
    """
    notify() iterates _HANDLERS and calls each one.
    We replace the real handler list with MagicMocks so we can verify
    which handlers were called and how they behaved.
    """

    def test_calls_all_handlers(self):
        handler_a = MagicMock()
        handler_b = MagicMock()
        outcome = _make_outcome()
        # Temporarily replace _HANDLERS with our two mocks
        with patch('notification._HANDLERS', [handler_a, handler_b]):
            notify(outcome)
        # Both handlers should have been called exactly once with the outcome
        handler_a.assert_called_once_with(outcome)
        handler_b.assert_called_once_with(outcome)

    def test_catches_handler_exception_without_crashing(self):
        # If one handler explodes, the others must still run.
        # __name__ is needed because notify's except block logs handler.__name__.
        bad_handler = MagicMock(side_effect=RuntimeError('boom'), __name__='bad_handler')
        good_handler = MagicMock(__name__='good_handler')
        outcome = _make_outcome()
        with patch('notification._HANDLERS', [bad_handler, good_handler]):
            notify(outcome)  # should NOT raise
        # good_handler still ran despite bad_handler blowing up
        good_handler.assert_called_once_with(outcome)
