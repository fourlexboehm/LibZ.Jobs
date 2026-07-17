const std = @import("std");
const debug = std.debug;
const assert = @import("assert");
const builtin = @import("builtin");

const Atomic = std.atomic.Value;
const Thread = std.Thread;
const FixedDeque = @import("fixed_deque.zig").FixedDeque;

pub const JobHandle = packed struct(u16) {
    index: u11,
    thread: u5,
};

pub const JobQueueConfig = struct {
    // For each thread a seperate queue will be created, this config dictates the size of that queue.
    max_jobs_per_thread: u11,

    // Dictates max number of threads
    max_threads: u16 = 32,

    // Amount of time to wait before trying to fetch a new job from the queue, If the queue is empty,
    // lowering this setting will result in high CPU and thus battery usage.
    idle_sleep_ns: u64 = 50,
};

/// Direct futex operations bypassing the std.Io VTable layer.
/// Uses platform-native syscalls for minimal overhead on the wake path.
const Futex = struct {
    const native_os = builtin.os.tag;
    const is_darwin = native_os.isDarwin();

    /// Blocks the calling thread if `ptr.*` == `expect`.
    /// Returns when woken by `wake()`, when `ptr.*` != `expect`, or when `timeout_ns` expires.
    fn timedWait(ptr: *const Atomic(u32), expect: u32, timeout_ns: ?u64) void {
        if (is_darwin) {
            const c = std.c;
            const flags: c.UL = .{
                .op = .COMPARE_AND_WAIT,
                .NO_ERRNO = true,
            };
            // __ulock_wait2 uses nanosecond timeouts (0 = infinite).
            // Available on macOS 11+. For older targets, fall back to __ulock_wait (microseconds).
            const darwin_has_wait2 = comptime builtin.os.version_range.semver.min.major >= 11;
            if (darwin_has_wait2) {
                _ = c.__ulock_wait2(flags, @ptrCast(&ptr.raw), expect, timeout_ns orelse 0, 0);
            } else {
                const us: u32 = if (timeout_ns) |ns|
                    std.math.lossyCast(u32, ns / std.time.ns_per_us)
                else
                    0;
                _ = c.__ulock_wait(flags, @ptrCast(&ptr.raw), expect, if (us == 0 and timeout_ns != null) 1 else us);
            }
        } else if (native_os == .linux) {
            const linux = std.os.linux;
            var ts_buf: linux.timespec = undefined;
            const ts: ?*linux.timespec = if (timeout_ns) |ns| blk: {
                ts_buf = .{
                    .sec = @intCast(ns / std.time.ns_per_s),
                    .nsec = @intCast(ns % std.time.ns_per_s),
                };
                break :blk &ts_buf;
            } else null;
            _ = linux.futex_4arg(
                @ptrCast(&ptr.raw),
                .{ .cmd = .WAIT, .private = true },
                expect,
                ts,
            );
        } else {
            @compileError("Futex.timedWait not implemented for this OS");
        }
    }

    /// Wakes up to `max_waiters` threads blocked in `timedWait` on the same address.
    fn wake(ptr: *const Atomic(u32), max_waiters: u32) void {
        debug.assert(max_waiters != 0);
        if (is_darwin) {
            const c = std.c;
            const flags: c.UL = .{
                .op = .COMPARE_AND_WAIT,
                .NO_ERRNO = true,
                .WAKE_ALL = max_waiters > 1,
            };
            while (true) {
                const status = c.__ulock_wake(flags, @ptrCast(&ptr.raw), 0);
                if (status >= 0) return;
                switch (@as(c.E, @enumFromInt(-status))) {
                    .INTR, .CANCELED => continue,
                    .NOENT => return, // no waiters
                    else => return,
                }
            }
        } else if (native_os == .linux) {
            const linux = std.os.linux;
            _ = linux.futex_3arg(
                @ptrCast(&ptr.raw),
                .{ .cmd = .WAKE, .private = true },
                @min(max_waiters, std.math.maxInt(i32)),
            );
        } else {
            @compileError("Futex.wake not implemented for this OS");
        }
    }
};

pub fn JobQueue(comptime config: JobQueueConfig) type {
    // Should atleast have 2 threads to be able to spawn
    comptime debug.assert(config.max_threads >= 2);

    const ExecData = [76]u8;
    const ExecFn = *const fn (*ExecData) void;

    const Job = struct {
        const Self = @This();

        // The function to call
        exec: ExecFn,

        // Parent job that spawned this job, can be executed in parallel of this job
        parent: ?JobHandle,

        // Jobs that need to be awaited before this job is completed, can be executed in parallel of this job
        job_count: Atomic(u16),

        // Child jobs, these need to be executed after this job has run
        child_count: Atomic(u16),
        child_jobs: [16]JobHandle,

        // Data passed to the function
        data: ExecData,

        pub fn init(comptime T: type, job: *const T) Self {
            var self: Self = .{
                .exec = undefined,
                .data = undefined,
                .parent = null,
                .job_count = Atomic(u16).init(1),
                .child_count = Atomic(u16).init(0),
                .child_jobs = @splat(undefined),
            };

            const bytes = std.mem.asBytes(job);
            @memcpy(self.data[0..bytes.len], bytes);

            const exec: *const fn (*T) void = &@field(T, "exec");

            self.exec = @as(ExecFn, @ptrCast(exec));

            return self;
        }

        pub fn isCompleted(self: *const Self) bool {
            return self.job_count.load(.monotonic) == 0;
        }
    };

    return struct {
        pub const max_jobs_per_thread = std.math.ceilPowerOfTwoPromote(u11, config.max_jobs_per_thread);
        pub const sleep_time_ns = config.idle_sleep_ns;
        const max_thread_count = config.max_threads;
        const max_thread_queue_count = max_thread_count + 1;

        const Self = @This();
        const Deque = FixedDeque(*Job, max_jobs_per_thread);

        // Simple job buffer with length tracking
        const Jobs = struct {
            buffer: [max_jobs_per_thread]Job align(128) = undefined,
            len: usize = 0,
        };

        const jobs_per_thread_mask = max_jobs_per_thread - 1;

        // Each thread has it's own queue, because we allow for job stealing from other threads the queue
        // itself is not thread local
        threadlocal var thread_queue_index: u32 = 0;

        /// Allocator used in init and deinit functions for allocating job buffers and queues
        allocator: std.mem.Allocator,

        /// Dynamic sleep time in nanoseconds, can be adjusted at runtime
        dynamic_sleep_ns: Atomic(u64) = Atomic(u64).init(config.idle_sleep_ns),

        /// Monotonic epoch counter for futex-based parking.
        /// Incremented on every schedule/notify to wake parked workers.
        /// Workers snapshot this before checking for work; if it changes
        /// between snapshot and futex_wait, the wait returns immediately
        /// (no lost wakes).
        park_epoch: Atomic(u32) = Atomic(u32).init(0),
        waiters: Atomic(u32) = Atomic(u32).init(0),
        queued_jobs: Atomic(u32) = Atomic(u32).init(0),

        // Each thread has it's own job buffer containing max_jobs_per_thread jobs.
        // Jobs are allocated from this buffer and deallocated to this buffer. is implemented as a ringbuffer.
        // JobHandle points to an index of this storage.
        jobs: []Jobs,

        // All the queues, one per spawned thread plus one for the main thread
        queues: []Deque,

        // All the threads, should be at most @min(getCpuCount() - 1, max_threads)
        threads: []Thread,

        // Main thread ID, stored so we can assert start is called from the main thread.
        main_thread: Atomic(u64) = Atomic(u64).init(0),

        // While true, the threads will keep trying to pick jobs from the queue, if set to false only
        // The picked up jobs will be completed
        is_running: Atomic(bool) = Atomic(bool).init(false),

        // Io for sleeping
        io: std.Io,

        /// Needs to be called before any other function
        pub fn init(allocator: std.mem.Allocator, io: std.Io) std.mem.Allocator.Error!Self {
            const thread_count = @min(max_thread_count, (Thread.getCpuCount() catch 2) - 1);
            const thread_queue_count = thread_count + 1;

            const self: Self = .{
                .allocator = allocator,
                .jobs = try allocator.alloc(Jobs, thread_queue_count),
                .queues = try allocator.alloc(Deque, thread_queue_count),
                .threads = try allocator.alloc(Thread, thread_count),
                .io = io,
            };

            for (self.queues, self.jobs) |*queue, *jobs| {
                queue.* = Deque.init();
                jobs.* = .{};
            }

            return self;
        }

        /// Needs to be called as last to cleanup memory
        pub fn deinit(self: *Self) void {
            const is_running = self.is_running.load(.monotonic);
            debug.assert(is_running == false);

            self.allocator.free(self.jobs);
            self.allocator.free(self.queues);
            self.allocator.free(self.threads);
        }

        /// Starts the threads, from this call onwards the threads will try to pickup jobs from their
        /// queues.
        pub fn start(self: *Self) !void {
            const current_thread = Thread.getCurrentId();
            const prev_thread = self.main_thread.swap(current_thread, .monotonic);
            debug.assert(prev_thread == 0);

            const was_running = self.is_running.swap(true, .monotonic);
            debug.assert(was_running == false);

            for (self.threads, 0..) |*thread, i| {
                thread.* = try Thread.spawn(.{}, run, .{ self, @as(u32, @intCast(i + 1)) });
            }
        }

        /// Stops the threads from listening to the queues, they will not pickup any new jobs from the
        /// queue, meaning that not all jobs will be finished.
        pub fn stop(self: *Self) void {
            const was_running = self.is_running.swap(false, .monotonic);
            debug.assert(was_running);
            self.notifyAll();
        }

        /// Joins all threads again, you can call this before stop but than a Job on another thread
        /// will need to call stop, otherwise this will wait indefinatley
        pub fn join(self: *Self) void {
            debug.assert(self.isMainThread());

            for (self.threads) |thread| {
                thread.join();
            }
        }

        /// Allocates a Job, needs to be called in order to get a valid handle that can than be scheduled
        pub fn allocate(self: *Self, job: anytype) JobHandle {
            const JobType = @TypeOf(job);

            var jobs = self.getThreadJobBuffer();
            defer jobs.len += 1;

            const job_index: u32 = @intCast(jobs.len & jobs_per_thread_mask);

            jobs.buffer[job_index] = Job.init(JobType, &job);

            return .{
                .index = @intCast(job_index),
                .thread = @intCast(thread_queue_index),
            };
        }

        /// Puts the job in a queue so it can be picked up by a thread
        pub fn schedule(self: *Self, handle: JobHandle) void {
            debug.assert(handle.thread == thread_queue_index);

            const queue = self.getThreadQueue();
            const job = self.getJobFromBuffer(handle);

            _ = self.queued_jobs.fetchAdd(1, .acq_rel);
            queue.push(job);
            self.notifyOne();
        }

        /// awaits the job to finish and while not finished yet will work on other jobs.
        pub fn wait(self: *Self, handle: JobHandle) void {
            debug.assert(handle.thread == thread_queue_index);

            const job = self.getJobFromBuffer(handle);
            while (!job.isCompleted()) {
                self.execNextJob();
            }
        }

        /// Waits without parking the caller. Intended for realtime threads that
        /// must continue helping until the target job completes.
        pub fn waitRealtime(self: *Self, handle: JobHandle) void {
            debug.assert(handle.thread == thread_queue_index);

            const job = self.getJobFromBuffer(handle);
            while (!job.isCompleted()) {
                if (!self.tryExecNextJob()) std.atomic.spinLoopHint();
            }
        }

        /// awaits the job to finish and while not finished yet will work on other jobs. This will
        /// return the result of the job. A simpler and slightly faster way of calling 'wait' and then 'result'.
        pub fn waitResult(self: *Self, T: type, handle: JobHandle) T {
            debug.assert(handle.thread == thread_queue_index);

            const job = self.getJobFromBuffer(handle);
            while (!job.isCompleted()) {
                self.execNextJob();
            }
            return std.mem.bytesToValue(T, &job.data);
        }

        /// returns the result of a completed job. asserts the job has actually been completed.
        pub fn result(self: *Self, T: type, handle: JobHandle) T {
            debug.assert(self.isCompleted(handle));
            const job = self.getJobFromBuffer(handle);
            return std.mem.bytesToValue(T, &job.data);
        }

        /// adds a job that will run after the main job has completed, multiple jobs can be added, they
        /// will be executed after the main job in same order of calling this function.
        pub fn continueWith(self: *Self, handle: JobHandle, continuation_handle: JobHandle) void {
            debug.assert(handle.thread == thread_queue_index);
            debug.assert(handle.thread == continuation_handle.thread);

            const parent = self.getJobFromBuffer(handle);
            const prev = parent.child_count.fetchAdd(1, .monotonic);
            parent.child_jobs[prev] = continuation_handle;
            debug.assert(prev < 16);
        }

        /// allows to await multiple jobs through a single job. once the finish_handle is awaited, all other jobs are
        /// guaranteed to be finished as well, though it does not enforce execution order. the 'finish job' function could be executed
        /// as first but will not be set to completed until all related jobs have finished executing.
        pub fn finishWith(self: *Self, handle: JobHandle, finish_handle: JobHandle) void {
            debug.assert(handle.thread == thread_queue_index);
            debug.assert(handle.thread == finish_handle.thread);

            const child = self.getJobFromBuffer(handle);
            const parent = self.getJobFromBuffer(finish_handle);
            _ = parent.job_count.fetchAdd(1, .monotonic);

            child.parent = finish_handle;
        }

        /// returns of the current thread is the main thread
        pub fn isMainThread(self: *Self) bool {
            const current_thread = Thread.getCurrentId();
            const main_thread = self.main_thread.load(.monotonic);
            debug.assert(main_thread != 0);

            return main_thread == current_thread;
        }

        /// returns whether the passed job has been completed
        pub fn isCompleted(self: *const Self, handle: JobHandle) bool {
            debug.assert(thread_queue_index == handle.thread);

            const job = self.getJobFromBufferConst(handle);
            return job.isCompleted();
        }

        /// Sets the idle sleep time in nanoseconds
        pub fn setSleepNs(self: *Self, ns: u64) void {
            self.dynamic_sleep_ns.store(ns, .monotonic);
        }

        fn run(self: *Self, queue_index: u32) void {
            debug.assert(thread_queue_index == 0);

            thread_queue_index = queue_index;

            while (self.is_running.load(.monotonic)) {
                self.execNextJob();
            }
        }

        fn execNextJob(self: *Self) void {
            if (!self.tryExecNextJob()) self.parkWorker();
        }

        fn tryExecNextJob(self: *Self) bool {
            if (self.getJob()) |job| {
                job.exec(&job.data);
                self.finishJob(job);
                return true;
            }
            return false;
        }

        fn getJob(self: *Self) ?*Job {
            const queue = self.getThreadQueue();
            if (queue.pop()) |job| {
                _ = self.queued_jobs.fetchSub(1, .acq_rel);
                return job;
            }

            for (1..self.queues.len) |i| {
                const index = (i + thread_queue_index) % self.queues.len;
                debug.assert(thread_queue_index != index);

                if (self.queues[index].steal()) |job| {
                    _ = self.queued_jobs.fetchSub(1, .acq_rel);
                    return job;
                }
            }

            return null;
        }

        fn parkWorker(self: *Self) void {
            const sleep_ns = self.dynamic_sleep_ns.load(.monotonic);
            var epoch = self.park_epoch.load(.acquire);
            if (self.queued_jobs.load(.acquire) == 0) {
                _ = self.waiters.fetchAdd(1, .monotonic);
                defer _ = self.waiters.fetchSub(1, .monotonic);
                while (self.is_running.load(.monotonic) and self.queued_jobs.load(.acquire) == 0) {
                    Futex.timedWait(&self.park_epoch, epoch, sleep_ns);
                    epoch = self.park_epoch.load(.acquire);
                }
            }
        }

        fn finishJob(self: *Self, job: *Job) void {
            const prev = job.job_count.fetchSub(1, .monotonic);
            if (prev == 1) {
                if (job.parent) |parent| {
                    const parent_job = self.getJobFromBuffer(parent);
                    self.finishJob(parent_job);
                }

                const child_count = job.child_count.load(.monotonic);
                if (child_count > 0) {
                    _ = self.queued_jobs.fetchAdd(@intCast(child_count), .acq_rel);
                }
                for (0..child_count) |i| {
                    const child_job = self.getJobFromBuffer(job.child_jobs[i]);
                    const queue = self.getThreadQueue();
                    queue.push(child_job);
                }
                if (child_count > 0) {
                    _ = self.park_epoch.fetchAdd(1, .release);
                    Futex.wake(&self.park_epoch, @intCast(child_count));
                } else {
                    self.notifyOne();
                }
            }
        }

        fn notifyOne(self: *Self) void {
            _ = self.park_epoch.fetchAdd(1, .release);
            Futex.wake(&self.park_epoch, 1);
        }

        fn notifyMany(self: *Self, count: u32) void {
            if (count == 0) return;
            _ = self.park_epoch.fetchAdd(1, .release);
            Futex.wake(&self.park_epoch, count);
        }

        fn notifyAll(self: *Self) void {
            _ = self.park_epoch.fetchAdd(1, .release);
            Futex.wake(&self.park_epoch, std.math.maxInt(u32));
        }

        inline fn getThreadQueue(self: *Self) *Deque {
            return &self.queues[thread_queue_index];
        }

        inline fn getThreadJobBuffer(self: *Self) *Jobs {
            return &self.jobs[thread_queue_index];
        }

        inline fn getJobFromBuffer(self: *Self, handle: JobHandle) *Job {
            return &self.jobs[handle.thread].buffer[handle.index];
        }

        inline fn getJobFromBufferConst(self: *const Self, handle: JobHandle) *const Job {
            return &self.jobs[handle.thread].buffer[handle.index];
        }
    };
}

test "waitRealtime executes work instead of parking" {
    const Queue = JobQueue(.{
        .max_jobs_per_thread = 8,
        .max_threads = 2,
        .idle_sleep_ns = std.time.ns_per_s,
    });
    const Blocker = struct {
        started: *Atomic(u32),
        release: *Atomic(bool),

        pub fn exec(self: *@This()) void {
            _ = self.started.fetchAdd(1, .release);
            while (!self.release.load(.acquire)) std.atomic.spinLoopHint();
        }
    };
    const RecordThread = struct {
        executor: *Atomic(u64),

        pub fn exec(self: *@This()) void {
            self.executor.store(Thread.getCurrentId(), .release);
        }
    };

    var queue = try Queue.init(std.testing.allocator, std.testing.io);
    defer queue.deinit();
    try queue.start();

    var release = Atomic(bool).init(false);
    var started = Atomic(u32).init(0);
    defer {
        release.store(true, .release);
        queue.stop();
        queue.join();
    }

    for (0..queue.threads.len) |_| {
        const blocker = queue.allocate(Blocker{ .started = &started, .release = &release });
        queue.schedule(blocker);
    }
    while (started.load(.acquire) != queue.threads.len) std.atomic.spinLoopHint();

    var executor = Atomic(u64).init(0);
    const target = queue.allocate(RecordThread{ .executor = &executor });
    queue.schedule(target);
    queue.waitRealtime(target);

    try std.testing.expectEqual(Thread.getCurrentId(), executor.load(.acquire));
}
