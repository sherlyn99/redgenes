from pathlib import Path
from redgenes.sql_connection import TRN
from redgenes.exceptions import PatchDirectoryNotFound, PatchFileExecutionError


def get_patch_list(patch_dir):
    """Returns the list of patch files in order."""
    # Check if patch_dir exists and is a directory because
    # Path(file_path).glob('*.sql') does report error if file_path does not exist
    path_patch_dir = Path(patch_dir)

    if path_patch_dir.exists() and path_patch_dir.is_dir():
        patch_list = sorted(path_patch_dir.glob("*.sql"), key=lambda x: int(x.stem))
        return patch_list
    else:
        raise PatchDirectoryNotFound(f"Directory not found: {patch_dir}")


def execute_patch_file(patch: Path):
    """Execute one patch file."""
    try:
        with open(patch) as f:
            sql_script = f.read()
    except Exception as e:
        raise PatchFileExecutionError(f"Cannot open {patch}")

    with TRN:
        TRN.executescript(sql_script)


def initialize_db():
    """Update patch files in settings table and execute new patches."""
    # Get patch file lists
    patch_list = get_patch_list("./redgenes/support_files")
    patch_ids = [[int(patch.stem)] for patch in patch_list]  # for TRN.add(many=True)
    patch_dict = {int(patch.stem): patch for patch in patch_list}
    patch_init = patch_list[0]

    # Creat the settings table by running the first patch
    execute_patch_file(patch_init)

    with TRN:
        # Update the settings table for new patches
        sql = "insert or ignore into settings (patch_id) values (?)"
        TRN.add(sql, patch_ids, many=True)

        # Set the execution status of the first patch to be 1 if applicable
        sql = "update settings set executed = 1, modified_at = current_timestamp where patch_id = ? and executed = 0"
        TRN.add(sql, [0])

        # Get the list of unexecuted sql scripts
        sql = "select patch_id from settings where executed = 0"
        TRN.add(sql)
        patch_ids_to_execute = TRN.execute_fetchflatten()

    # Execute unexecuted patches if applicable
    if patch_ids_to_execute:
        for patch_id in patch_ids_to_execute:
            patch = patch_dict[patch_id]
            try:
                execute_patch_file(patch)
            except Exception as e:
                raise PatchFileExecutionError(
                    f"There is an error in running patch file {patch_id}"
                )
            else:
                with TRN:
                    sql = "update settings set executed = 1, modified_at = current_timestamp where patch_id = ?"
                    TRN.add(sql, [patch_id])
                    TRN.execute()
