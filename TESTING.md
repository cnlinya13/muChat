# Testing matrix (generic -- no real messages, files, or accounts)

Run each case against a throwaway conversation, one item at a time;
only the account owner's own verdict counts as a pass.

- [ ] Text: plain question in, correct reply out, single reply.
- [ ] Image: image in, worker describes the actual image; text+emoji
      and image+text combos behave the same way.
- [ ] Voice with server transcription: reply answers the transcription
      and says so; voice without transcription: worker says plainly it
      cannot be understood, never guesses.
- [ ] File: original extension preserved on disk; worker opens the
      original by type (text direct, office docs via a document reader,
      other binaries by name+size only).
- [ ] Video: thumbnail/extracted frames described; worker says plainly
      the full video cannot be watched.
- [ ] Quote: quoted content restated first, then the new text answered;
      unresolved quotes are stated as unresolved, never guessed.
- [ ] Burst: several messages in seconds merge into one reply via the
      pre-outbox supplement re-check (boundary: outbox written).
- [ ] Follow-up correction: a correction message changes the answer.
- [ ] Outbound image: sent via the official driver, opens/downloads
      normally in the client.
- [ ] Daemon ack: file batches ack at once with filenames listed;
      text-only batches ack after a short wait with generic text.
- [ ] Restart: no duplicate reply after a daemon restart.

Known open item (not closed): multi-file bursts occasionally lose one
file somewhere between client upload and iLink delivery; the loss
moves between files across rounds and is under investigation.
