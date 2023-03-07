from smbclient import link, open_file, register_session, remove, rename, stat, symlink
import posixpath
from shutil import copyfileobj
# Optional - register the server with explicit credentials
host="172.16.0.2"
register_session(host, username=r"dagbti\atamovrb", password="Ragimatamov@yandex.ru")

def join_path(path):
    return f"//{posixpath.join(host, path.lstrip('/'))}"

local_filepath = 'utils.py'
remote_filepath_name = r"/public/Управление информационных технологий/Отдел разработки программного обеспечения/_ежедневка_2022/test.py"
remote_filepath=join_path(remote_filepath_name)
with open(local_filepath, "rb") as f, open_file(remote_filepath, mode="wb") as g:
    copyfileobj(f, g)

# # Read an existing file as text (credentials only needed for the first request to the server if not registered.)
# with open_file(r"\\server\share\file.txt", username="admin", password="pass") as fd:
#     file_contents = fd.read()
#
# # Read an existing file as bytes
# with open_file(r"\\server\share\file.txt", mode="rb") as fd:
#     file_bytes = fd.read()
#
# # Create a file and write to it
# with open_file(r"\\server\share\file.txt", mode="w") as fd:
#     fd.write("content")
#
# # Write data to the end of an existing file
# with open_file(r"\\server\share\file.txt", mode="a") as fd:
#     fd.write("\ndata at the end")
#
# # Delete a file
# remove(r"\\server\share\file.txt")
#
# # Get info about a file
# stat(r"\\server\share\file.txt")
#
# # Create a symbolic link
# symlink(r"\\server\share\directory", r"\\server\share\link")
#
# # Create a hard link
# link(r"\\server\share\file.txt", r"\\server\share\hard-link.txt")
#
# # Rename a file
# rename(r"\\server\share\old-name.txt", r"\\server\share\new-name.txt")