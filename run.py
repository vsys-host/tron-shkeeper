import threading

import app


#
# Wallet encryption
#

app.wallet_encryption.setup_encryption()

#
# Refresh best server
#

refresh_best_server_thread = threading.Thread(
    daemon=True,
    name="Refresh best server",
    target=app.connection_manager.ConnectionManager.manager().refresh_best_server_thread_handler,
)
refresh_best_server_thread.start()

#
# Flask
#

server = app.create_app()

#
# Block scanner
#

block_scanner = app.block_scanner.BlockScanner()

block_scanner_thread = threading.Thread(
    daemon=True,
    name="Block Scanner",
    target=block_scanner,
)
block_scanner_thread.start()

block_scanner_stats_thread = threading.Thread(
    daemon=True,
    name="Scanner Stats",
    target=app.block_scanner.block_scanner_stats,
    args=(block_scanner,),
)
block_scanner_stats_thread.start()
