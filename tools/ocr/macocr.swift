// macocr — batch OCR via Apple's Vision framework.
//
// Why this exists: the gallery images dealers publish frequently include a photo or
// render of the window sticker / "Vehicle Highlights" slide. Those carry the option
// codes, package names and true MSRP that the listing feed itself omits. Reading them
// needs OCR, and the three obvious options each had a problem:
//
//   * Claude vision  — best quality, but costs per image and needs an API key. At
//                      ~115k cars x dozens of images it is not a batch-the-fleet tool.
//   * tesseract      — free, but not installed, needs brew, and is markedly worse on
//                      the dense multi-column tables a Monroney label uses.
//   * Apple Vision   — free, on-device, ships with macOS, and is very strong on exactly
//                      this kind of printed text. No key, no network, no quota.
//
// So Apple Vision is the batch engine. Callers may still escalate a low-confidence or
// ambiguous image to a paid model; see backend/vision/image_text.py.
//
// Usage:  macocr <image> [<image> ...]        -> JSON array on stdout
//         macocr --paths-from <file>          -> one path per line (avoids ARG_MAX)
//
// Output is a JSON array; one object per input, always in input order, always the same
// length as the input. Failures are reported per-image via `error` rather than aborting
// the batch, because a single unreadable file must not cost us the other 40.
//
// Build: swiftc -O tools/ocr/macocr.swift -o tools/ocr/bin/macocr

import Foundation
import CoreImage
import Vision

struct OCRResult: Codable {
    let path: String
    let text: String
    let lines: [String]
    // Normalised bounding box per line, parallel to `lines`, as [x, y, width, height]
    // with origin at the BOTTOM-left (Vision's convention). Callers need this because a
    // Monroney is laid out in columns: "Total MSRP" and the dollar figure beside it are
    // separate observations, so a purely line-based reader pairs a label with whatever
    // text happens to follow it in reading order rather than with its own value.
    let boxes: [[Double]]
    let meanConfidence: Double
    let lineCount: Int
    let error: String?
}

func empty(_ path: String, _ error: String?) -> OCRResult {
    OCRResult(path: path, text: "", lines: [], boxes: [], meanConfidence: 0, lineCount: 0, error: error)
}

func recognize(_ path: String) -> OCRResult {
    guard FileManager.default.fileExists(atPath: path) else {
        return empty(path, "not_found")
    }
    guard let image = CIImage(contentsOf: URL(fileURLWithPath: path)) else {
        return empty(path, "decode_failed")
    }

    let request = VNRecognizeTextRequest()
    request.recognitionLevel = .accurate
    // Language correction "fixes" option codes into English words — ZPK becomes ZIP,
    // 5DN becomes SDN. The whole point of reading a sticker is those codes, so it stays off.
    request.usesLanguageCorrection = false
    request.recognitionLanguages = ["en-US"]

    let handler = VNImageRequestHandler(ciImage: image, options: [:])
    do {
        try handler.perform([request])
    } catch {
        return empty(path, "vision_failed: \(error.localizedDescription)")
    }

    guard let observations = request.results else { return empty(path, nil) }

    var lines: [String] = []
    var boxes: [[Double]] = []
    var confidences: [Double] = []
    for observation in observations {
        guard let candidate = observation.topCandidates(1).first else { continue }
        let s = candidate.string.trimmingCharacters(in: .whitespacesAndNewlines)
        if s.isEmpty { continue }
        lines.append(s)
        let b = observation.boundingBox
        boxes.append([
            Double(b.origin.x), Double(b.origin.y),
            Double(b.size.width), Double(b.size.height),
        ])
        confidences.append(Double(candidate.confidence))
    }

    let mean = confidences.isEmpty ? 0 : confidences.reduce(0, +) / Double(confidences.count)
    return OCRResult(
        path: path,
        text: lines.joined(separator: "\n"),
        lines: lines,
        boxes: boxes,
        meanConfidence: mean,
        lineCount: lines.count,
        error: nil
    )
}

// --- entry point ---------------------------------------------------------------

var paths: [String] = []
let argv = Array(CommandLine.arguments.dropFirst())

if argv.first == "--paths-from", argv.count >= 2 {
    guard let blob = try? String(contentsOfFile: argv[1], encoding: .utf8) else {
        FileHandle.standardError.write("macocr: cannot read \(argv[1])\n".data(using: .utf8)!)
        exit(2)
    }
    paths = blob.split(separator: "\n").map(String.init).filter { !$0.isEmpty }
} else {
    paths = argv
}

guard !paths.isEmpty else {
    FileHandle.standardError.write("usage: macocr <image>... | --paths-from <file>\n".data(using: .utf8)!)
    exit(2)
}

let results = paths.map(recognize)

let encoder = JSONEncoder()
encoder.outputFormatting = [.withoutEscapingSlashes]
if let data = try? encoder.encode(results), let json = String(data: data, encoding: .utf8) {
    print(json)
} else {
    FileHandle.standardError.write("macocr: encode failed\n".data(using: .utf8)!)
    exit(1)
}
