from pypeline.Stage import Stage
from pypeline.Decorators import pipelineStage, consumes, provides

@pipelineStage
class IntegerStage(Stage):
    def __init__(self):
        super().__init__()

    @consumes('None')
    def setInteger(self, *args, **kwargs):
        self.theInteger = 1

    @provides('None')
    def printInteger(self, *args, **kwargs):
        print(self.theInteger)
