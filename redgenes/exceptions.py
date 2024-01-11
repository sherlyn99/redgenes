class Error(Exception):
    pass


class PatchError(Error):
    pass


class PatchDirectoryNotFound(PatchError):
    pass


class InvalidPatchFile(PatchError):
    pass


class PatchFileExecutionError(PatchError):
    pass


class AnnotationError(Error):
    pass


class ProdigalError(AnnotationError):
    pass


class KofamscanError(AnnotationError):
    pass


class BarrnapError(AnnotationError):
    pass


class InputError(Error):
    pass


class InvalidInputTsv(InputError):
    pass


class InvalidFna(InputError):
    pass
