from pypeline.Stage import Stage
from pypeline.Decorators import pipelineStage, consumes, provides

@pipelineStage
class IntegerStage(Stage):
    def __init__(self, theInteger=1):
        super().__init__()
        self.theIneger = theInteger

    def configure(self, configuration):
        # As defined in the schema.yaml
        self.theInteger = configuration['number']

    @consumes('None')
    def setInteger(self, *args, **kwargs):
        # 'None' Consumer needed for successful execution
        pass

    @provides('None')
    def printInteger(self, *args, **kwargs):
        print(self.theInteger)
