// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
//
// The APL v2.0:
//
//---------------------------------------------------------------------------
//   Copyright (c) 2007-2026 Broadcom. All Rights Reserved.
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//       https://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.
//---------------------------------------------------------------------------
//
// The MPL v2.0:
//
//---------------------------------------------------------------------------
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
//
//  Copyright (c) 2007-2026 Broadcom. All Rights Reserved.
//---------------------------------------------------------------------------

using System;
using System.Buffers;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using RabbitMQ.Client.Util;
using static RabbitMQ.Client.Impl.Framing;

namespace RabbitMQ.Client
{
    internal sealed class OwnedBodySegment : ReadOnlySequenceSegment<byte>, IDisposable
    {
        private readonly IMemoryOwner<byte> _owner;

        internal OwnedBodySegment(IMemoryOwner<byte> owner, int length, long runningIndex)
        {
            _owner = owner;
            Memory = owner.Memory.Slice(0, length);
            RunningIndex = runningIndex;
        }

        internal void SetNext(OwnedBodySegment next) => Next = next;

        public void Dispose()
        {
            _owner.Dispose();
            (Next as IDisposable)?.Dispose();
        }
    }

    internal struct OutgoingFrame : IDisposable
    {
        private IMemoryOwner<byte>? _methodAndHeader;
        private readonly int _methodAndHeaderLength;
        private IMemoryOwner<byte>? _body;
        private readonly int _bodyLength;
        private readonly int _maxBodyPayloadBytes;
        private readonly ushort _channelNumber;
        private OwnedBodySegment? _bodyHead;
        private OwnedBodySegment? _bodyTail;
        private ReadOnlySequence<byte> _externalBody;
        private IDisposable? _externalBodyDisposable;

        internal OutgoingFrame(
            IMemoryOwner<byte> methodAndHeader,
            int methodAndHeaderLength)
        {
            _methodAndHeader = methodAndHeader;
            _methodAndHeaderLength = methodAndHeaderLength;
            _body = null;
            _bodyLength = 0;
            _channelNumber = 0;
            _maxBodyPayloadBytes = 0;
            _bodyHead = null;
            _bodyTail = null;
            _externalBody = default;
            _externalBodyDisposable = null;
            Size = methodAndHeaderLength;
        }

        internal OutgoingFrame(
            IMemoryOwner<byte> methodAndHeader,
            int methodAndHeaderLength,
            IMemoryOwner<byte> body,
            int bodyLength,
            ushort channelNumber,
            int maxBodyPayloadBytes,
            int totalSize)
        {
            _methodAndHeader = methodAndHeader;
            _methodAndHeaderLength = methodAndHeaderLength;
            _body = body;
            _bodyLength = bodyLength;
            _channelNumber = channelNumber;
            _maxBodyPayloadBytes = maxBodyPayloadBytes;
            _bodyHead = null;
            _bodyTail = null;
            _externalBody = default;
            _externalBodyDisposable = null;
            Size = totalSize;
        }

        internal OutgoingFrame(
            IMemoryOwner<byte> methodAndHeader,
            int methodAndHeaderLength,
            OwnedBodySegment bodyHead,
            OwnedBodySegment bodyTail,
            int bodyLength,
            ushort channelNumber,
            int maxBodyPayloadBytes,
            int totalSize)
        {
            _methodAndHeader = methodAndHeader;
            _methodAndHeaderLength = methodAndHeaderLength;
            _body = null;
            _bodyLength = bodyLength;
            _channelNumber = channelNumber;
            _maxBodyPayloadBytes = maxBodyPayloadBytes;
            _bodyHead = bodyHead;
            _bodyTail = bodyTail;
            _externalBody = default;
            _externalBodyDisposable = null;
            Size = totalSize;
        }

        internal OutgoingFrame(
            IMemoryOwner<byte> methodAndHeader,
            int methodAndHeaderLength,
            ReadOnlySequence<byte> externalBody,
            IDisposable externalBodyDisposable,
            int bodyLength,
            ushort channelNumber,
            int maxBodyPayloadBytes,
            int totalSize)
        {
            _methodAndHeader = methodAndHeader;
            _methodAndHeaderLength = methodAndHeaderLength;
            _body = null;
            _bodyLength = bodyLength;
            _channelNumber = channelNumber;
            _maxBodyPayloadBytes = maxBodyPayloadBytes;
            _bodyHead = null;
            _bodyTail = null;
            _externalBody = externalBody;
            _externalBodyDisposable = externalBodyDisposable;
            Size = totalSize;
        }

        internal int Size { get; }

        internal readonly void WriteTo(IBufferWriter<byte> writer)
        {
            Debug.Assert(_methodAndHeader is not null);
            writer.Write(_methodAndHeader!.Memory.Span.Slice(0, _methodAndHeaderLength));

            if (_bodyLength == 0)
            {
                return;
            }

            if (_bodyHead is not null)
            {
                var sequence = new ReadOnlySequence<byte>(_bodyHead, 0, _bodyTail!, _bodyTail!.Memory.Length);
                WriteBodySequence(writer, sequence);
            }
            else if (_externalBodyDisposable is not null)
            {
                WriteBodySequence(writer, _externalBody);
            }
            else
            {
                Debug.Assert(_body is not null);
                ReadOnlySpan<byte> bodySpan = _body!.Memory.Span.Slice(0, _bodyLength);
                int remainingBodyBytes = bodySpan.Length;
                int bodyOffset = 0;

                while (remainingBodyBytes > 0)
                {
                    int payloadSize = remainingBodyBytes > _maxBodyPayloadBytes ? _maxBodyPayloadBytes : remainingBodyBytes;
                    BodySegment.WriteTo(writer, _channelNumber, bodySpan.Slice(bodyOffset, payloadSize));
                    remainingBodyBytes -= payloadSize;
                    bodyOffset += payloadSize;
                }
            }
        }

        private readonly void WriteBodySequence(IBufferWriter<byte> writer, ReadOnlySequence<byte> sequence)
        {
            int remaining = _bodyLength;
            SequencePosition position = sequence.Start;

            while (remaining > 0)
            {
                int framePayload = remaining > _maxBodyPayloadBytes ? _maxBodyPayloadBytes : remaining;
                WriteBodyFrame(writer, _channelNumber, sequence.Slice(position, framePayload));
                position = sequence.GetPosition(framePayload, position);
                remaining -= framePayload;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static void WriteBodyFrame(IBufferWriter<byte> writer, ushort channel, ReadOnlySequence<byte> payload)
        {
            int payloadLength = (int)payload.Length;

            Span<byte> header = writer.GetSpan(7);
            header[0] = Constants.FrameBody;
            NetworkOrderSerializer.WriteUInt16(ref header.GetOffset(1), channel);
            NetworkOrderSerializer.WriteUInt32(ref header.GetOffset(3), (uint)payloadLength);
            writer.Advance(7);

            foreach (ReadOnlyMemory<byte> segment in payload)
            {
                writer.Write(segment.Span);
            }

            Span<byte> end = writer.GetSpan(1);
            end[0] = Constants.FrameEnd;
            writer.Advance(1);
        }

        public void Dispose()
        {
            IMemoryOwner<byte>? memoryOwner = _methodAndHeader;
            _methodAndHeader = null;
            if (memoryOwner != null)
            {
                memoryOwner.Dispose();
                _methodAndHeader = default;
                _body?.Dispose();
                _body = null;
                _bodyHead?.Dispose();
                _bodyHead = null;
                _bodyTail = null;
                _externalBodyDisposable?.Dispose();
                _externalBodyDisposable = null;
                _externalBody = default;
            }
        }
    }
}
