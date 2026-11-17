"""Runner resource ownership and shutdown ordering without database access."""

import runpy
from pathlib import Path
from unittest.mock import Mock, patch

import pytest


@pytest.mark.parametrize('failure', [None, RuntimeError('worker failed'), KeyboardInterrupt()])
def test_runner_stops_workers_before_closing_tracker(monkeypatch, failure):
    directory = Path(__file__).resolve().parents[1] / 'examples'
    monkeypatch.syspath_prepend(str(directory))
    with patch('sqlalchemy.create_engine') as create:
        namespace = runpy.run_path(str(directory / 'runner_marts.py'))
        create.assert_not_called()
    main = namespace['main']
    events = []
    engine, tracker, handler = Mock(), Mock(), Mock()

    def run(**kwargs):
        assert kwargs == {'poll_interval': 60}
        events.append('worker')
        if failure is not None:
            raise failure

    handler.run_forever.side_effect = run
    tracker.close.side_effect = lambda: events.append('tracker.close')
    engine.dispose.side_effect = lambda: events.append('engine.dispose')
    main.__globals__.update(create_engine=Mock(return_value=engine),
                            MergeTracker=Mock(return_value=tracker),
                            ProductDailySales=Mock(return_value=handler),
                            request_stop=lambda: events.append('stop'))
    with patch('signal.signal') as register:
        if failure is None:
            main()
        else:
            with pytest.raises(type(failure)):
                main()
        import signal
        assert register.call_count == 4
        assert register.call_args_list[2].args == (signal.SIGTERM, register.return_value)
        assert register.call_args_list[3].args == (signal.SIGINT, register.return_value)
    assert events == ['worker', 'stop', 'tracker.close', 'engine.dispose']