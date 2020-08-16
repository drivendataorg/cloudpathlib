# from .s3.s3backend import S3Backend
# from .s3.s3path import S3Path

# # do we even care about doing dispatch?
# def infer_from_path(path):
#     backends = [
#         (S3Backend, S3Path),
#     ]

#     for backend_cls, path_cls in backends:
#         if backend_cls.is_valid_path(path):
#             return backend_cls, path_cls

#     else:
#         raise ValueError(f"Path ({path}) is not valid; it must start with one of {list(backends.keys())}")
