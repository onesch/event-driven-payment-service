import asyncio
from unittest.mock import call, Mock

import pytest
from sqlalchemy.exc import SQLAlchemyError

from src.payment_service.models.payment import PaymentStatus
from src.payment_worker.services.payment_processor import PaymentProcessor


def _executed_statement(session) -> str:
    """Compile the UPDATE statement passed to ``session.execute()``."""
    session.execute.assert_awaited_once()

    statement = session.execute.await_args.args[0]

    return str(statement.compile(compile_kwargs={"literal_binds": True}))


# Test cases for the simulated processing delay


@pytest.mark.asyncio
async def test_process_simulates_processing_delay(
    processor_sleep,
    processing_delay,
    processor_session,
):
    await PaymentProcessor.process(payment_id=42)

    processing_delay.assert_called_once_with(2, 5)
    processor_sleep.assert_awaited_once_with(3.0)


@pytest.mark.asyncio
async def test_process_waits_before_updating_payment(
    processor_sleep,
    processing_delay,
    processor_session_maker,
):
    manager = Mock()
    manager.attach_mock(processor_sleep, "sleep")
    manager.attach_mock(processor_session_maker, "session_maker")

    await PaymentProcessor.process(payment_id=42)

    assert manager.mock_calls == [
        call.sleep(processing_delay.return_value),
        call.session_maker(),
    ]


# Test cases for selecting the payment result


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("roll", "expected_status"),
    [
        (0.0, PaymentStatus.SUCCEEDED),
        (0.5, PaymentStatus.SUCCEEDED),
        (0.8999, PaymentStatus.SUCCEEDED),
        (0.9, PaymentStatus.FAILED),
        (0.95, PaymentStatus.FAILED),
        (0.999, PaymentStatus.FAILED),
    ],
    ids=[
        "zero_roll",
        "middle_roll",
        "just_below_threshold",
        "exact_threshold",
        "high_roll",
        "almost_one",
    ],
)
async def test_process_marks_payment_with_resulting_status(
    processor_session,
    processor_sleep,
    processing_delay,
    payment_outcome,
    roll,
    expected_status,
):
    payment_outcome.return_value = roll

    await PaymentProcessor.process(payment_id=42)

    compiled = _executed_statement(processor_session)

    assert f"SET status='{expected_status.value}'" in compiled


# Test cases for the UPDATE statement built by the processor


@pytest.mark.asyncio
async def test_process_updates_only_requested_payment(
    processor_session,
    processor_sleep,
    processing_delay,
):
    await PaymentProcessor.process(payment_id=7)

    compiled = _executed_statement(processor_session)

    assert "UPDATE payments" in compiled
    assert "WHERE payments.id = 7" in compiled


@pytest.mark.asyncio
async def test_process_does_not_overwrite_processed_payment(
    processor_session,
    processor_sleep,
    processing_delay,
):
    """Only pending payments may transition to a final status."""
    await PaymentProcessor.process(payment_id=42)

    compiled = _executed_statement(processor_session)

    assert "payments.status = 'pending'" in compiled


# Test cases for the database transaction


@pytest.mark.asyncio
async def test_process_commits_payment_update(
    processor_session,
    processor_sleep,
    processing_delay,
):
    await PaymentProcessor.process(payment_id=42)

    processor_session.execute.assert_awaited_once()
    processor_session.commit.assert_awaited_once_with()


@pytest.mark.asyncio
async def test_process_releases_database_session(
    processor_session,
    processor_sleep,
    processing_delay,
):
    await PaymentProcessor.process(payment_id=42)

    processor_session.__aenter__.assert_awaited_once_with()
    processor_session.__aexit__.assert_awaited_once_with(None, None, None)


@pytest.mark.asyncio
async def test_process_database_error_skips_commit_and_propagates(
    processor_session,
    processor_sleep,
    processing_delay,
):
    processor_session.execute.side_effect = SQLAlchemyError("connection lost")

    with pytest.raises(SQLAlchemyError):
        await PaymentProcessor.process(payment_id=42)

    processor_session.commit.assert_not_awaited()
    # The session must still be released when the UPDATE fails.
    processor_session.__aexit__.assert_awaited_once()


@pytest.mark.asyncio
async def test_process_commit_error_propagates(
    processor_session,
    processor_sleep,
    processing_delay,
):
    processor_session.commit.side_effect = SQLAlchemyError("commit failed")

    with pytest.raises(SQLAlchemyError):
        await PaymentProcessor.process(payment_id=42)

    processor_session.execute.assert_awaited_once()
    processor_session.__aexit__.assert_awaited_once()


@pytest.mark.asyncio
async def test_process_cancelled_during_delay_skips_database(
    processor_session_maker,
    processor_sleep,
    processing_delay,
):
    processor_sleep.side_effect = asyncio.CancelledError()

    with pytest.raises(asyncio.CancelledError):
        await PaymentProcessor.process(payment_id=42)

    processor_session_maker.assert_not_called()
