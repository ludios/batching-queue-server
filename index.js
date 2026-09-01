const readlines = require("./readlines");

const http = require("node:http");
const fs = require("node:fs");
const url = require("node:url");

const item_file = process.argv[2];
const position_file = process.argv[3];
let port = 31417;
if (process.env.PORT) {
	port = Number(process.env.PORT);
}
let cycle = true;
if (process.env.CYCLE === "false") {
	cycle = false;
}

let rl = new readlines.LineByLine(item_file);
let line_position = 0;
function write_line_position() {
	fs.writeFileSync(position_file, Buffer.from(String(line_position)));
}
if (fs.existsSync(position_file)) {
	line_position = Number(fs.readFileSync(position_file).toString("utf-8").trim());

	let skip_lines = line_position;
	while (skip_lines--) {
		rl.next();
	}
}
function back_to_top() {
	line_position = 0;
	write_line_position();
	rl = new readlines.LineByLine(item_file);
}

const server = http.createServer(async (req, res) => {
	if (req.method === "GET") {
		res.writeHead(200, { "Content-Type": "text/plain" });

		const parts = url.parse(req.url, true);
		const query = parts.query;
		let max = query.max || 1;
		max = Number(max);
		const body = [];

		while (max--) {
			const line = rl.next();
			if (line !== false) {
				line_position += 1;
				body.push(line);
				body.push("\n");
			} else {
				if (cycle) {
					// Loop the queue back around to the top in case some items failed
					back_to_top();
				}

				break;
			}
		}
	
		write_line_position();

		if (body.length) {
			res.end(body.join(""));
		} else {
			// Wait a bit to slow down clients who might have run out of items
			setTimeout(() => {
				res.end();
			}, 1000);
		}
	}
}).listen(port);
