import logging

BEHAVE_DEBUG_ON_ERROR = True


def setup_debug_on_error(userdata):
    global BEHAVE_DEBUG_ON_ERROR
    BEHAVE_DEBUG_ON_ERROR = userdata.getbool("BEHAVE_DEBUG_ON_ERROR")


def before_all(context):
    if "artifacts-directory" in context.config.userdata:
        context.artifact_directory = context.config.userdata["artifacts-directory"]
    if not context.config.log_capture:
        logging.basicConfig(level=logging.DEBUG)
    setup_debug_on_error(context.config.userdata)


def after_step(context, step):
    if BEHAVE_DEBUG_ON_ERROR and step.status == "failed":
        # -- ENTER DEBUGGER: Zoom in on failure location.
        # NOTE: Use IPython debugger, same for pdb (basic python debugger).
        import ipdb
        ipdb.post_mortem(step.exc_traceback)
