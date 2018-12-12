// Copyright 2017 Google Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "sling/myelin/macro-assembler.h"

#include <stddef.h>

#include "sling/base/logging.h"
#include "sling/base/macros.h"
#include "sling/myelin/compute.h"

namespace sling {
namespace myelin {

using namespace jit;

#ifdef NDEBUG
// Base register for data instance.
static Register datareg = rbp;

// Register used for profile timestamp.
static Register tsreg = r15;
#else
// Do not use rbp in debug mode to avoid confusing the debugger.
static Register datareg = r15;
static Register tsreg = r14;
#endif

Register Registers::try_alloc() {
  for (int r = 0; r < kNumRegisters; ++r) {
    if (!used(r)) {
      use(r);
      return Register::from_code(r);
    }
  }
  return no_reg;
}

Register Registers::alloc() {
  Register r = try_alloc();
  CHECK(r.is_valid()) << "Register overflow";
  return r;
}

Register Registers::try_alloc_preserved() {
  for (int r = 0; r < kNumRegisters; ++r) {
    if (!used(r) && preserved(r)) {
      use(r);
      return Register::from_code(r);
    }
  }
  return no_reg;
}

Register Registers::alloc_preserved() {
  Register r = try_alloc_preserved();
  CHECK(r.is_valid()) << "Register overflow";
  return r;
}

Register Registers::alloc_preferred(Register r) {
  if (!used(r)) {
    use(r);
    return r;
  } else {
    return alloc();
  }
}

Register Registers::alloc_fixed(Register r) {
  CHECK(!used(r)) << "Register already used";
  use(r);
  return r;
}

Register Registers::alloc_temp() {
  if (!used(r10)) return alloc_fixed(r10);
  if (!used(r11)) return alloc_fixed(r11);
  LOG(FATAL) << "Temp register overflow";
  return no_reg;
}

Register Registers::arg(int n) {
  Register r;
  switch (n) {
    case 0: r = rax; break;
    case 1: r = arg_reg_1; break;
    case 2: r = arg_reg_2; break;
    case 3: r = arg_reg_3; break;
    case 4: r = arg_reg_4; break;
    case 5: r = arg_reg_5; break;
    case 6: r = arg_reg_6; break;
    default: LOG(FATAL) << "Only six argument registers";
  }
  return alloc_fixed(r);
}

void Registers::reserve(int r) {
  CHECK(!saved(r)) << r;
  CHECK(used(r)) << r;
  saved_regs_ |= (1 << r);
  used_regs_ &= ~(1 << r);
}

void Registers::free(int r) {
  CHECK(saved(r)) << r;
  CHECK(!used(r)) << r;
  saved_regs_ &= ~(1 << r);
  used_regs_ |= (1 << r);
}

bool Registers::usage(int n) {
  switch (n) {
    case 13: reserve(r15); FALLTHROUGH_INTENDED;
    case 12: reserve(r14); FALLTHROUGH_INTENDED;
    case 11: reserve(r13); FALLTHROUGH_INTENDED;
    case 10: reserve(r12); FALLTHROUGH_INTENDED;
    case 9: reserve(rbx); FALLTHROUGH_INTENDED;
    case 8: case 7: case 6: case 5: case 4: case 3: case 2: case 1: case 0:
      return true;
  }
  return false;
}

int Registers::num_free() const {
  int n = 0;
  for (int r = 0; r < kNumRegisters; ++r) {
    if (!used(r)) n++;
  }
  return n;
}

int SIMDRegisters::try_alloc(bool extended) {
  for (int r = 0; r < (extended ? kNumZRegisters : kNumXRegisters); ++r) {
    if ((used_regs_ & (1 << r)) == 0) {
      use(r);
      return r;
    }
  }
  return -1;
}

int SIMDRegisters::alloc(bool extended) {
  int r = try_alloc(extended);
  CHECK(r != -1) << "SIMD register overflow";
  return r;
}

OpmaskRegister OpmaskRegisters::try_alloc() {
  for (int r = 0; r < kNumRegisters; ++r) {
    OpmaskRegister k = OpmaskRegister::from_code(r);
    if (!used(k)) {
      use(k);
      return k;
    }
  }
  return no_opmask_reg;
}

OpmaskRegister OpmaskRegisters::alloc() {
  OpmaskRegister k = try_alloc();
  CHECK(k.is_valid()) << "Opmask register overflow";
  return k;
}

void StaticData::AddData(const void *buffer, int size, int repeat) {
  const uint8 *ptr = static_cast<const uint8 *>(buffer);
  for (int n = 0; n < repeat; ++n) {
    data_.insert(data_.end(), ptr, ptr + size);
  }
}

bool StaticData::Equals(const void *data, int size, int repeat) const {
  if (size * repeat != data_.size()) return false;
  const uint8 *p1 = data_.data();
  for (int i = 0; i < repeat; ++i) {
    const uint8 *p2 = static_cast<const uint8 *>(data);
    for (int j = 0; j < size; ++j) {
      if (*p1++ != *p2++) return false;
    }
  }
  return true;
}

void StaticData::Generate(MacroAssembler *masm) {
  // Align output.
  masm->DataAlign(alignment_);

  // Bind label to the address of the generated data block.
  masm->bind(&location_);

  // Emit data block.
  for (uint8 byte : data_) masm->db(byte);
}

MacroAssembler::MacroAssembler(void *buffer, int buffer_size,
                               const Options &options)
    : Assembler(buffer, buffer_size), options_(options) {}

MacroAssembler::~MacroAssembler() {
  for (auto *d : data_blocks_) delete d;
}

Register MacroAssembler::instance() const {
  return datareg;
}

void MacroAssembler::Prologue() {
  // Zero upper part of YMM register if CPU needs it to avoid AVX-SSE transition
  // penalties.
  if (CPU::VZeroNeeded() && Enabled(AVX)) {
    vzeroupper();
  }

  // Reserve data instance register.
  rr_.reserve(datareg);
  rr_.use(datareg);

  // Reserve timestamp register.
  if (options_.profiling) {
    rr_.reserve(tsreg);
    rr_.use(tsreg);
  }

  // Save preserved registers on stack.
  if (rr_.saved(rbp)) pushq(rbp);
  if (rr_.saved(rbx)) pushq(rbx);
  if (rr_.saved(r12)) pushq(r12);
  if (rr_.saved(r13)) pushq(r13);
  if (rr_.saved(r14)) pushq(r14);
  if (rr_.saved(r15)) pushq(r15);

  // Get argument.
  if (!datareg.is(arg_reg_1)) {
    movq(datareg, arg_reg_1);
  }

  // Get initial timestamp counter if timing instrumentation is active.
  if (options_.profiling) {
    rdtsc();
    shlq(rdx, Immediate(32));
    orq(rax, rdx);
    movq(tsreg, rax);
  }
}

void MacroAssembler::Epilogue() {
  // Restore preserved registers from stack.
  if (rr_.saved(r15)) popq(r15);
  if (rr_.saved(r14)) popq(r14);
  if (rr_.saved(r13)) popq(r13);
  if (rr_.saved(r12)) popq(r12);
  if (rr_.saved(rbx)) popq(rbx);
  if (rr_.saved(rbp)) popq(rbp);

  // Zero upper part of YMM register if CPU needs it to avoid AVX-SSE transition
  // penalties.
  if (CPU::VZeroNeeded() && Enabled(AVX)) {
    vzeroupper();
  }

  // Generate return instruction.
  ret(0);

  // Release timing register.
  if (options_.profiling) {
    rr_.release(tsreg);
    rr_.free(tsreg);
  }

  // Release data instance register.
  rr_.release(datareg);
  rr_.free(datareg);
}

StaticData *MacroAssembler::CreateDataBlock(int alignment) {
  StaticData *data = new StaticData(alignment);
  data_blocks_.push_back(data);
  return data;
}

StaticData *MacroAssembler::FindDataBlock(
    const void *data, int size, int repeat) {
  for (StaticData *sd : data_blocks_) {
    if (sd->Equals(data, size, repeat)) return sd;
  }
  return nullptr;
}

void MacroAssembler::GenerateDataBlocks() {
  for (StaticData *sd : data_blocks_) {
    sd->Generate(this);
  }
}

void MacroAssembler::LoopStart(jit::Label *label) {
  bind(label);
}

void MacroAssembler::LoadTensorAddress(Register dst, Tensor *tensor) {
  if (tensor->IsGlobal()) {
    DCHECK(tensor->data() != nullptr);
    load_extern(dst, tensor->data(), tensor->name());
    if (tensor->ref()) {
      movq(dst, Operand(dst));
    }
  } else if (tensor->offset() == 0) {
    if (tensor->ref()) {
      movq(dst, Operand(datareg));
    } else {
      movq(dst, datareg);
    }
  } else {
    DCHECK(tensor->offset() != -1) << tensor->name();
    if (tensor->ref()) {
      movq(dst, Operand(datareg, tensor->offset()));
    } else {
      leaq(dst, Operand(datareg, tensor->offset()));
    }
  }
}

void MacroAssembler::LoadTensorAddress(Register dst, Tensor *tensor,
                                       Tensor *indices) {
  if (indices == nullptr) {
    LoadTensorAddress(dst, tensor);
  } else {
    CHECK_LE(indices->elements(), tensor->rank());
    CHECK_EQ(indices->type(), DT_INT32);
    if (indices->constant()) {
      std::vector<int> index;
      CHECK(indices->GetData(&index));
      int offset = tensor->offset(index);
      if (tensor->IsGlobal() || tensor->ref()) {
        LoadTensorAddress(dst, tensor);
        if (offset != 0) addq(dst, Immediate(offset));
      } else {
        int disp = tensor->offset() + offset;
        leaq(dst, Operand(instance(), disp));
      }
    } else {
      Register iptr = rr_.alloc();
      Register acc = rr_.alloc();
      if (indices->rank() < 2) {
        LoadTensorAddress(dst, tensor);
        if (indices->ref()) {
          movq(iptr, Operand(instance(), indices->offset()));
          movsxlq(acc, Operand(iptr));
        } else if (indices->IsGlobal()) {
          load_extern(iptr, indices->data(), indices->name());
          movsxlq(acc, Operand(iptr));
        } else {
          movsxlq(acc, Operand(instance(), indices->offset()));
        }
        Multiply(acc, tensor->stride(0));
        addq(dst, acc);
      } else {
        LoadTensorAddress(dst, tensor);
        LoadTensorAddress(iptr, indices);
        for (int i = 0; i < indices->elements(); ++i) {
          movsxlq(acc, Operand(iptr, i * sizeof(int)));
          Multiply(acc, tensor->stride(i));
          addq(dst, acc);
        }
      }
      rr_.release(iptr);
      rr_.release(acc);
    }
  }
}

void MacroAssembler::Copy(Register dst, int ddisp,
                          Register src, int sdisp,
                          int size) {
  if (size > 0 && size < 16) {
    // Copy small blocks with move instructions.
    Register acc = rr_.alloc();
    int disp = 0;
    int left = size;
    while (left >= 8) {
      movq(acc, Operand(src, sdisp + disp));
      movq(Operand(dst, ddisp + disp), acc);
      disp += 8;
      left -= 8;
    }
    while (left >= 4) {
      movl(acc, Operand(src, sdisp + disp));
      movl(Operand(dst, ddisp + disp), acc);
      disp += 4;
      left -= 4;
    }
    while (left >= 2) {
      movw(acc, Operand(src, sdisp + disp));
      movw(Operand(dst, ddisp + disp), acc);
      disp += 2;
      left -= 2;
    }
    while (left >= 1) {
      movb(acc, Operand(src, sdisp + disp));
      movb(Operand(dst, ddisp + disp), acc);
      disp += 1;
      left -= 1;
    }
    rr_.release(acc);
  } else {
    // Save registers if needed.
    bool restore_rsi = false;
    bool restore_rdi = false;
    bool restore_rcx = false;
    if (!src.is(rsi) && rr_.used(rsi)) {
      pushq(rsi);
      restore_rsi = true;
    }
    if (!dst.is(rdi) && rr_.used(rdi)) {
      pushq(rdi);
      restore_rdi = true;
    }
    if (rr_.used(rcx)) {
      pushq(rcx);
      restore_rcx = true;
    }

    // Set up source and destination.
    if (src.is(rdi) && dst.is(rdi)) {
      xchgq(dst, src);
      if (ddisp != 0) addq(rdi, Immediate(ddisp));
      if (sdisp != 0) addq(rsi, Immediate(sdisp));
    } else {
      if (dst.is(rdi)) {
        if (ddisp != 0) addq(rdi, Immediate(ddisp));
      } else {
        if (ddisp != 0) {
          leaq(rdi, Operand(dst, ddisp));
        } else {
          movq(rdi, dst);
        }
      }

      if (src.is(rsi)) {
        if (sdisp != 0) addq(rsi, Immediate(sdisp));
      } else {
        if (sdisp != 0) {
          leaq(rsi, Operand(src, sdisp));
        } else {
          movq(rsi, src);
        }
      }
    }

    // Set up size.
    movq(rcx, Immediate(size));

    // Copy data.
    repmovsb();

    // Restore registers if needed.
    if (restore_rcx) popq(rcx);
    if (restore_rdi) popq(rdi);
    if (restore_rsi) popq(rsi);
  }
}

void MacroAssembler::LoadInteger(jit::Register dst, jit::Register base,
                                 jit::Register index, Type type) {
  switch (type) {
    case DT_INT8:
      movsxbq(dst, Operand(base, index, times_1));
      break;

    case DT_UINT8:
      movb(dst, Operand(base, index, times_1));
      break;

    case DT_INT16:
      movsxwq(dst, Operand(base, index, times_2));
      break;

    case DT_UINT16:
      movw(dst, Operand(base, index, times_2));
      break;

    case DT_INT32:
      movsxlq(dst, Operand(base, index, times_4));
      break;

    case DT_INT64:
      movq(dst, Operand(base, index, times_8));
      break;

    default:
      LOG(FATAL) << "Invalid integer type: " << type;
  }
}

void MacroAssembler::StoreInteger(jit::Register base, jit::Register index,
                                  jit::Register src, Type type) {
  switch (type) {
    case DT_INT8:
    case DT_UINT8:
      movb(Operand(base, index, times_1), src);
      break;

    case DT_INT16:
    case DT_UINT16:
      movw(Operand(base, index, times_2), src);
      break;

    case DT_INT32:
      movl(Operand(base, index, times_4), src);
      break;

    case DT_INT64:
      movq(Operand(base, index, times_8), src);
      break;

    default:
      LOG(FATAL) << "Invalid integer type: " << type;
  }
}

void MacroAssembler::Multiply(jit::Register reg, int64 scalar) {
  if (scalar == 0) {
    xorq(reg, reg);
  } else if (scalar < 0) {
    imulq(reg, reg, Immediate(scalar));
  } else if (scalar != 1) {
    // Check if scalar is power of two.
    int shift = 0;
    int64 value = 1;
    while (value < scalar) {
      value <<= 1;
      shift++;
    }
    if (value == scalar) {
      shlq(reg, Immediate(shift));
    } else {
      imulq(reg, reg, Immediate(scalar));
    }
  }
}

OpmaskRegister MacroAssembler::LoadMask(int n, OpmaskRegister k) {
  if (!k.is_valid()) k = kk_.alloc();
  Register r = rr_.alloc();
  movq(r, Immediate((1 << n) - 1));
  kmovq(k, r);
  rr_.release(r);
  return k;
}

void MacroAssembler::Accumulate(Reduction op, Type type,
                                XMMRegister acc, XMMRegister r) {
  bool avx = Enabled(AVX);
  switch (type) {
    case DT_FLOAT:
      switch (op) {
        case REDUCE_ADD:
          if (avx) {
            vaddps(acc, acc, r);
          } else {
            addps(acc, r);
          }
          break;
        case REDUCE_MUL:
          if (avx) {
            vmulps(acc, acc, r);
          } else {
            mulps(acc, r);
          }
          break;
        case REDUCE_MIN:
          if (avx) {
            vminps(acc, acc, r);
          } else {
            minps(acc, r);
          }
          break;
        case REDUCE_MAX:
          if (avx) {
            vmaxps(acc, acc, r);
          } else {
            maxps(acc, r);
          }
          break;
      }
      break;
    case DT_DOUBLE:
      switch (op) {
        case REDUCE_ADD:
          if (avx) {
            vaddpd(acc, acc, r);
          } else {
            addpd(acc, r);
          }
          break;
        case REDUCE_MUL:
          if (avx) {
            vmulpd(acc, acc, r);
          } else {
            mulpd(acc, r);
          }
          break;
        case REDUCE_MIN:
          if (avx) {
            vminpd(acc, acc, r);
          } else {
            minpd(acc, r);
          }
          break;
        case REDUCE_MAX:
          if (avx) {
            vmaxpd(acc, acc, r);
          } else {
            maxpd(acc, r);
          }
          break;
      }
      break;
    default:
      LOG(FATAL) << "Reduction for type not supported";
  }
}

void MacroAssembler::Accumulate(Reduction op, Type type,
                                YMMRegister acc, YMMRegister r) {
  switch (type) {
    case DT_FLOAT:
      switch (op) {
        case REDUCE_ADD:
          vaddps(acc, acc, r);
          break;
        case REDUCE_MUL:
          vmulps(acc, acc, r);
          break;
        case REDUCE_MIN:
          vminps(acc, acc, r);
          break;
        case REDUCE_MAX:
          vmaxps(acc, acc, r);
          break;
      }
      break;
    case DT_DOUBLE:
      switch (op) {
        case REDUCE_ADD:
          vaddpd(acc, acc, r);
          break;
        case REDUCE_MUL:
          vmulpd(acc, acc, r);
          break;
        case REDUCE_MIN:
          vminpd(acc, acc, r);
          break;
        case REDUCE_MAX:
          vmaxpd(acc, acc, r);
          break;
      }
      break;
    default:
      LOG(FATAL) << "Reduction for type not supported";
  }
}

void MacroAssembler::Accumulate(Reduction op, Type type,
                                ZMMRegister acc, ZMMRegister r) {
  switch (type) {
    case DT_FLOAT:
      switch (op) {
        case REDUCE_ADD:
          vaddps(acc, acc, r);
          break;
        case REDUCE_MUL:
          vmulps(acc, acc, r);
          break;
        case REDUCE_MIN:
          vminps(acc, acc, r);
          break;
        case REDUCE_MAX:
          vmaxps(acc, acc, r);
          break;
      }
      break;
    case DT_DOUBLE:
      switch (op) {
        case REDUCE_ADD:
          vaddpd(acc, acc, r);
          break;
        case REDUCE_MUL:
          vmulpd(acc, acc, r);
          break;
        case REDUCE_MIN:
          vminpd(acc, acc, r);
          break;
        case REDUCE_MAX:
          vmaxpd(acc, acc, r);
          break;
      }
      break;
    default:
      LOG(FATAL) << "Reduction for type not supported";
  }
}

void MacroAssembler::Reduce(Reduction op, Type type,
                            XMMRegister acc, XMMRegister aux) {
  int n = (128 / 8) / TypeTraits::of(type).size();
  if (CPU::Enabled(AVX)) {
    switch (n) {
      case 4:
        vpermil(type, aux, acc, 0x0E);
        Accumulate(op, type, acc, aux);
        FALLTHROUGH_INTENDED;
      case 2:
        vpermil(type, aux, acc, 0x01);
        Accumulate(op, type, acc, aux);
        break;
      default:
        LOG(FATAL) << "Reduction not supported";
    }
  } else if (CPU::Enabled(SSE3) && n == 4) {
    movshdup(aux, acc);
    Accumulate(op, type, acc, aux);
    movhlps(aux, acc);
    Accumulate(op, type, acc, aux);
  } else if (n == 4) {
    movaps(aux, acc);
    shufps(aux, acc, 0xB1);
    Accumulate(op, type, acc, aux);
    if (CPU::Enabled(SSE2)) {
      movhlps(aux, acc);
    } else {
      movaps(aux, acc);
      shufps(aux, acc, 0x03);
    }
    Accumulate(op, type, acc, aux);
  } else if (CPU::Enabled(SSE2) && n == 2) {
    movapd(aux, acc);
    shufpd(aux, acc, 1);
    Accumulate(op, type, acc, aux);
  } else {
    LOG(FATAL) << "Reduction not supported";
  }
}

void MacroAssembler::Reduce(Reduction op, Type type,
                            YMMRegister acc, YMMRegister aux) {
  int n = (256 / 8) / TypeTraits::of(type).size();
  vperm2f128(aux, acc, acc, 1);
  Accumulate(op, type, acc, aux);
  switch (n) {
    case 8:
      vpermil(type, aux, acc, 0x0E);
      Accumulate(op, type, acc, aux);
      FALLTHROUGH_INTENDED;
    case 4:
      vpermil(type, aux, acc, 0x01);
      Accumulate(op, type, acc, aux);
      break;
    default:
      LOG(FATAL) << "Reduction not supported";
  }
}

void MacroAssembler::Reduce(Reduction op, Type type,
                            ZMMRegister acc, ZMMRegister aux) {
  int n = (512 / 8) / TypeTraits::of(type).size();
  vshuff32x4(aux, acc, acc, 0x0E);
  Accumulate(op, type, acc, aux);
  vshuff32x4(aux, acc, acc, 0xB1);
  Accumulate(op, type, acc, aux);
  switch (n) {
    case 16:
      vpermil(type, aux, acc, 0x0E);
      Accumulate(op, type, acc, aux);
      vpermil(type, aux, acc, 0x01);
      Accumulate(op, type, acc, aux);
      break;
    case 8:
      vpermil(type, aux, acc, 0x01);
      Accumulate(op, type, acc, aux);
      break;
    default:
      LOG(FATAL) << "Reduction not supported";
  }
}

void MacroAssembler::UpdateCounter(int64 *counter, int64 value) {
  CHECK(!rr_.used(rdi));
  movp(rdi, counter);
  lock();
  addq(Operand(rdi), Immediate(value));
}

void MacroAssembler::StartTask(int offset, int32 id, int32 index,
                               jit::Label *entry) {
  // Check that runtime supports parallel execution.
  CHECK(runtime_->SupportsAsync())
      << "Runtime does not support asynchronous execution";

  // Fill out task structure.
  Register acc = rr_.alloc();
  leaq(arg_reg_1, Operand(datareg, offset));
  leaq(acc, Operand(entry));
  movq(Operand(arg_reg_1, offsetof(Task, func)), acc);
  movq(Operand(arg_reg_1, offsetof(Task, arg)), datareg);
  movl(Operand(arg_reg_1, offsetof(Task, id)), Immediate(id));
  movl(Operand(arg_reg_1, offsetof(Task, index)), Immediate(index));

  // Call runtime to start task.
  load_extern(acc, reinterpret_cast<void *>(runtime_->StartTaskFunc()),
              "MyelinStartTask");
  call(acc);

  rr_.release(acc);
}

void MacroAssembler::WaitForTask(int offset) {
  // Check that runtime supports parallel execution.
  CHECK(runtime_->SupportsAsync())
      << "Runtime does not support asynchronous execution";

  // Call runtime to wait for task to complete.
  Register acc = rr_.alloc();
  leaq(arg_reg_1, Operand(datareg, offset));
  load_extern(acc, reinterpret_cast<void *>(runtime_->WaitTaskFunc()),
              "MyelinWaitTask");
  call(acc);
  rr_.release(acc);
}

void MacroAssembler::CallInstanceFunction(void (*func)(void *),
                                          const string &symbol) {
  if (func != nullptr) {
    Register acc = rr_.alloc();
    movq(arg_reg_1, datareg);
    load_extern(acc, reinterpret_cast<void *>(func), symbol);
    call(acc);
    rr_.release(acc);
  }
}

void MacroAssembler::IncrementInvocations(int offset) {
  if (options_.ref_profiler()) {
    CHECK(!rr_.used(rdi));
    movq(rdi, Operand(datareg, offset));
    incq(Operand(rdi));
  } else {
    incq(Operand(datareg, offset));
  }
}

void MacroAssembler::TimeStep(int offset, int disp) {
  // Timing instrumentation must be active.
  CHECK(options_.profiling);
  CHECK(!rr_.used(rax));
  CHECK(!rr_.used(rdx));

  // Get current time stamp (rax).
  rdtsc();
  shlq(rdx, Immediate(32));
  orq(rax, rdx);

  // Compute time elapsed (rdx).
  movq(rdx, rax);
  subq(rdx, tsreg);

  // Add elapsed time to timing block.
  if (options_.ref_profiler()) {
    CHECK(!rr_.used(rdi));
    movq(rdi, Operand(datareg, offset));
    addq(Operand(rdi, disp), rdx);
  } else {
    addq(Operand(datareg, offset + disp), rdx);
  }

  // Store new timestamp.
  movq(tsreg, rax);
}

void MacroAssembler::ResetRegisterUsage() {
  rr_.reset();
  mm_.reset();
  kk_.reset();
  rr_.use(datareg);
  if (options_.profiling) rr_.use(tsreg);
}

void MacroAssembler::vpermil(Type type, XMMRegister dst,
                             XMMRegister src, int8_t imm8) {
  if (TypeTraits::of(type).size() == 8) {
    vpermilpd(dst, src, imm8);
  } else {
    vpermilps(dst, src, imm8);
  }
}

void MacroAssembler::vpermil(Type type, YMMRegister dst,
                             YMMRegister src, int8_t imm8) {
  if (TypeTraits::of(type).size() == 8) {
    vpermilpd(dst, src, imm8);
  } else {
    vpermilps(dst, src, imm8);
  }
}

void MacroAssembler::vpermil(Type type, ZMMRegister dst,
                             ZMMRegister src, int8_t imm8) {
  if (TypeTraits::of(type).size() == 8) {
    vpermilpd(dst, src, imm8);
  } else {
    vpermilps(dst, src, imm8);
  }
}

}  // namespace myelin
}  // namespace sling

