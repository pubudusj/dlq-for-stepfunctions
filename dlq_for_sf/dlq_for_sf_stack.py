from aws_cdk import (
    Duration,
    Stack,
    aws_sqs as sqs,
    CfnOutput,
    Stack,
    aws_iam as iam,
    aws_lambda as _lambda,
    aws_sqs as sqs,
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as tasks,
    aws_lambda_event_sources as event_sources,
)
from constructs import Construct


class DlqForSfStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        dl_queue = sqs.Queue(self, "DLQueue", queue_name="my-dlq")

        source_queue = sqs.Queue(
            self,
            "SourceQueue",
            queue_name="my-source-queue",
        )

        # Exception Lambda function that will be triggered by step function
        # Always raise exception
        error_lambda = _lambda.Function(
            self,
            "ErrorLambda",
            runtime=_lambda.Runtime.PYTHON_3_11,
            code=_lambda.Code.from_asset("src/error_lambda"),
            handler="index.handler",
            retry_attempts=0,
        )

        # Building Step function
        call_error_lambda = tasks.LambdaInvoke(
            scope=self,
            id="Call Error Lambda",
            lambda_function=error_lambda,
        )

        send_messge_to_dlq = tasks.CallAwsService(
            scope=self,
            id="Set message to DLQ",
            action="sendMessage",
            service="sqs",
            parameters={
                "QueueUrl.$": "$$.Execution.Input.metadata.sqs_dlq_url",
                "MessageBody.$": "$$.Execution.Input",
            },
            iam_resources=[dl_queue.queue_arn],
        )

        sf_definition = sfn.DefinitionBody.from_chainable(
            call_error_lambda.add_retry(
                max_attempts=None,
                errors=["States.ALL"],
            ).add_catch(
                handler=send_messge_to_dlq,
                errors=["States.ALL"],
            )
        )

        message_processor_state_machine = sfn.StateMachine(
            scope=self,
            id="MessageProcessorStateMachine",
            definition_body=sf_definition,
            timeout=Duration.minutes(2),
        )

        # Lambda to initialize step functions
        sf_exectuion_initializer_lambda = _lambda.Function(
            self,
            "SfExecutionInitializer",
            runtime=_lambda.Runtime.PYTHON_3_11,
            code=_lambda.Code.from_asset("src/sf_init_lambda"),
            handler="index.handler",
            retry_attempts=0,
            environment={
                "STATE_MACHINE_ARN": message_processor_state_machine.state_machine_arn,
                "DLQ_URL": dl_queue.queue_url,
            },
        )
        source_queue.grant_send_messages(sf_exectuion_initializer_lambda)
        sf_exectuion_initializer_lambda.add_event_source(
            event_sources.SqsEventSource(source_queue, batch_size=1)
        )

        # Allow Lambda to init SF execution
        message_processor_state_machine.grant_start_execution(
            sf_exectuion_initializer_lambda
        )

        # SF permission to send message to dlq
        dl_queue.grant_send_messages(message_processor_state_machine)
