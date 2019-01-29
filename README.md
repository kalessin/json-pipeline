Tool for defining dict lists or jsonlines batch processing pipelines.

See tests for usage on the first case. You can also process a jsonlines batch file using the command line tool.

For running tests:

    > nosetest3 tests/test_transform.py

Typically you build command line tool in this way:

```
from json_pipeline.transform import TransformScript as JPTransformScript

class TransformScript(JPTransformScript):
    PIPELINE = ...


if __name__ == '__main__':
    transform = TransformScript()
    transform.main()
```

You can skip definition of your subclass. In that case pipeline can be passed via command line.
