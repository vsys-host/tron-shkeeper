import threading

import app


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

#
# Flask
#

server = app.create_app()

if __name__ == '__main__':
    server.run(debug=app.config['DEBUG'], use_reloader=False, host="0.0.0.0", port=6000)
