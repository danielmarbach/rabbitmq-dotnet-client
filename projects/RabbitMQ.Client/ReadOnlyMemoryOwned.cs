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
using System.Threading;

namespace RabbitMQ.Client
{
    public sealed class ReadOnlyMemoryOwned<T> : IReadOnlyMemoryOwner<T>
    {
        private IMemoryOwner<T>? _memoryOwner;
        private Action? _onDispose;

        public ReadOnlyMemoryOwned(ReadOnlyMemory<T> memory)
        {
            Memory = memory;
        }

        public ReadOnlyMemoryOwned(ReadOnlyMemory<T> memory, Action onDispose)
        {
            Memory = memory;
            _onDispose = onDispose ?? throw new ArgumentNullException(nameof(onDispose));
        }

        public ReadOnlyMemoryOwned(IMemoryOwner<T> memoryOwner)
            : this(memoryOwner, memoryOwner?.Memory.Length ?? 0)
        {
        }

        public ReadOnlyMemoryOwned(IMemoryOwner<T> memoryOwner, int length)
        {
            if (memoryOwner is null)
            {
                throw new ArgumentNullException(nameof(memoryOwner));
            }

            Memory<T> memory = memoryOwner.Memory;
            if ((uint)length > (uint)memory.Length)
            {
                throw new ArgumentOutOfRangeException(nameof(length));
            }

            Memory = memory.Slice(0, length);
            _memoryOwner = memoryOwner;
        }

        public ReadOnlyMemory<T> Memory { get; }

        public void Dispose()
        {
            IMemoryOwner<T>? memoryOwner = Interlocked.Exchange(ref _memoryOwner, null);
            memoryOwner?.Dispose();

            Action? onDispose = Interlocked.Exchange(ref _onDispose, null);
            onDispose?.Invoke();
        }
    }

    public sealed class ReadOnlyMemoryOwned<T, TState>(ReadOnlyMemory<T> memory, TState state, Action<TState> onDispose)
        : IReadOnlyMemoryOwner<T>
    {
        private Action<TState>? _onDispose = onDispose ?? throw new ArgumentNullException(nameof(onDispose));

        public ReadOnlyMemory<T> Memory { get; } = memory;

        public void Dispose()
        {
            Action<TState>? onDispose = Interlocked.Exchange(ref _onDispose, null);
            onDispose?.Invoke(state);
        }
    }
}
