from pipeline import Pipeline

pipeline = Pipeline()
flow = pipeline.flow
default_parameters = {x.name: x.default for x in flow.parameters()}
test_parameters = pipeline.get_test_parameters(default_parameters)

state = flow.run(**test_parameters)

assert state.is_successful()
