"""Lambda function that always raise exception."""


def handler(event, context):
    raise Exception("Always return exception")
