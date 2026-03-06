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
using RabbitMQ.Client;
using Xunit;

namespace Test.Unit
{
    public class TestReadOnlyMemoryOwned
    {
        [Fact]
        public void WrapsIMemoryOwnerAndUsesRequestedLength()
        {
            var inner = new TrackedMemoryOwner(5);
            var sut = new ReadOnlyMemoryOwned<byte>(inner, 3);

            Assert.Equal(3, sut.Memory.Length);

            sut.Dispose();

            Assert.True(inner.Disposed);
        }

        [Fact]
        public void DisposeCallbackWithStateIsInvokedOnlyOnce()
        {
            var state = new DisposableState();
            var sut = new ReadOnlyMemoryOwned<byte, DisposableState>(new byte[] { 1, 2, 3 }, state, static s => s.DisposeCount++);

            sut.Dispose();
            sut.Dispose();

            Assert.Equal(1, state.DisposeCount);
        }

        private sealed class TrackedMemoryOwner : IMemoryOwner<byte>
        {
            private readonly byte[] _memory;

            public TrackedMemoryOwner(int size)
            {
                _memory = new byte[size];
            }

            public Memory<byte> Memory => _memory;

            public bool Disposed { get; private set; }

            public void Dispose()
            {
                Disposed = true;
            }
        }

        private sealed class DisposableState
        {
            public int DisposeCount { get; set; }
        }
    }
}
