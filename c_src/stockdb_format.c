#include <stdint.h>
#include "erl_nif.h"
#include <sys/mman.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <string.h>
#include <errno.h>
#include <stdio.h>
#include <unistd.h>
#include <time.h>


struct BitReader {
  unsigned char *bytes;
  unsigned size;
  unsigned offset;
};

static inline void bit_init(struct BitReader *br, unsigned char *bytes, unsigned size) {
  br->bytes = bytes;
  br->size = size;
  br->offset = 0;
}

static inline unsigned char bit_look(struct BitReader *br) {
  unsigned offset = br->offset;
  if(br->offset == br->size*8) return 0;
  return (br->bytes[offset / 8] >> (7 - (offset % 8))) & 1;
}

static inline unsigned char bit_get(struct BitReader *br) {
  unsigned char bit = bit_look(br);
  br->offset++;
  // fprintf(stderr, "Bit: %d\r\n", bit);
  return bit;
}

static inline uint64_t bits_get(struct BitReader *br, int bits) {
  int i;
  uint64_t result = 0;
  if(bits == 32 && br->offset % 8 == 0 && br->offset <= (br->size - 4)*8) {
    result = ntohl(*(uint32_t *)(&br->bytes[br->offset / 8]));
    br->offset += 32;
    return result;
  }
  while(bits >= 8 && br->offset % 8 == 0 && br->offset <= (br->size - 1)*8) {
    bits -= 8;
    result = (result << 8) | br->bytes[br->offset / 8];
    br->offset += 8;
  }
  for(i = 0; i < bits; i++) {
    result = (result << 1) | bit_get(br);
  }
  return result;
}

static inline void bits_align(struct BitReader *br) {
  if(br->offset % 8 == 0) return;
  br->offset = ((br->offset / 8)+1)*8;
  if(br->offset > br->size*8) {
    br->offset = br->size*8;
  }
}

static inline unsigned bits_byte_offset(struct BitReader *br) {
  return br->offset / 8;
}


static inline uint64_t leb128_decode_unsigned(struct BitReader *br) {
  uint64_t result = 0;
  int shift = 0;
  int move_on = 1;
  while(move_on) {
    move_on = bit_get(br);
    uint64_t chunk = bits_get(br, 7); 
    result |= (chunk << shift);
    // fprintf(stderr, "ULeb: %d, %llu, %llu\r\n", move_on, chunk, result);
    shift += 7;
  }
  return result;
}

static inline uint64_t leb128_decode_signed(struct BitReader *br) {
  int64_t result = 0;
  int shift = 0;
  int move_on = 1;
  int sign = 0;
  while(move_on) {
    move_on = bit_get(br);
    sign = bit_look(br);
    uint64_t chunk = bits_get(br, 7); 
    result |= (chunk << shift);
    // fprintf(stderr, "SLeb: %d, %llu, %lld\r\n", move_on, chunk, sign ? result | - (1 << shift) :  result);
    shift += 7;
  }
  if(sign) {
    result |= - (1 << shift);
  }
  return result;
}


static inline int64_t decode_delta(struct BitReader *br) {
  if(bit_get(br)) {
    return leb128_decode_signed(br);
  } else {
    return 0;
  }
}


static ERL_NIF_TERM
read_one_row(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
  ErlNifBinary bin;
  unsigned int depth;
  if(!enif_inspect_binary(env, argv[0], &bin)) return enif_make_badarg(env);
  if(!enif_get_uint(env, argv[1], &depth)) return enif_make_badarg(env);

  unsigned shift = bin.size;

  struct BitReader br;
  bit_init(&br, bin.data, bin.size);
  
  ErlNifUInt64 timestamp;
  
  ERL_NIF_TERM bid[depth];
  ERL_NIF_TERM ask[depth];
  int i;
  
  ERL_NIF_TERM tag;
  
  if(bit_get(&br)) {
    if(bit_get(&br)) {
      return enif_make_badarg(env);
    }
    tag = enif_make_atom(env, "md");
    timestamp = bits_get(&br, 62);
    for(i = 0; i < depth; i++) {
      bid[i] = enif_make_tuple2(env,
        enif_make_int(env, (int32_t)bits_get(&br, 32)),
        enif_make_uint(env, (uint32_t)bits_get(&br, 32))
        );
    }
    for(i = 0; i < depth; i++) {
      ask[i] = enif_make_tuple2(env,
        enif_make_int(env, (int32_t)bits_get(&br, 32)),
        enif_make_uint(env, (uint32_t)bits_get(&br, 32))
        );
    }
  } else {
    tag = enif_make_atom(env, "delta");
    timestamp = leb128_decode_unsigned(&br);
    for(i = 0; i < depth; i++) {
      bid[i] = enif_make_tuple2(env,
        enif_make_int(env, (int32_t)decode_delta(&br)),
        enif_make_uint(env, (uint32_t)decode_delta(&br))
        );
    }
    for(i = 0; i < depth; i++) {
      ask[i] = enif_make_tuple2(env,
        enif_make_int(env, (int32_t)decode_delta(&br)),
        enif_make_uint(env, (uint32_t)decode_delta(&br))
        );
    }
  }
  
  bits_align(&br);
  shift = bits_byte_offset(&br);
  
  return enif_make_tuple3(env,
    enif_make_atom(env, "ok"),
    enif_make_tuple4(env,
      tag,
      enif_make_uint64(env, timestamp),
      enif_make_list_from_array(env, bid, depth),
      enif_make_list_from_array(env, ask, depth)
    ),
    enif_make_sub_binary(env, argv[0], shift, bin.size - shift)
    );
}


static int upgrade(ErlNifEnv* env, void** priv_data, void** old_priv_data, ERL_NIF_TERM load_info) {
  return 0;
}

static int reload(ErlNifEnv* env, void** priv_data, ERL_NIF_TERM load_info) {
  return 0;
}

static ErlNifFunc stockdb_funcs[] =
{
  {"read_one_row", 2, read_one_row}
};


ERL_NIF_INIT(stockdb_format, stockdb_funcs, NULL, reload, upgrade, NULL)
