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
#include <arpa/inet.h>

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
  uint64_t result = 0;
  if(br->offset + bits > br->size*8) return 0;
  
  if(bits <= 8 && br->offset & 0x7) {
    while(bits) {
      result = (result << 1) | bit_get(br);
      bits--;
    }
    return result;
  }
  
  if(bits == 32 && br->offset % 8 == 0) {
    result = ntohl(*(uint32_t *)(&br->bytes[br->offset / 8]));
    br->offset += 32;
    return result;
  }

  while(bits) {
    while(bits >= 8 && !(br->offset & 0x7)) {
      bits -= 8;
      result = (result << 8) | br->bytes[br->offset >> 3];
      br->offset += 8;
    }
    if(bits) {
      result = (result << 1) | bit_get(br);
      bits--;
    }
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
make_error(ErlNifEnv* env, const char *err) {
  return enif_make_tuple2(env, enif_make_atom(env, "error"), enif_make_atom(env, err));
}

static ERL_NIF_TERM
read_one_row(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
  ErlNifBinary bin;
  unsigned int depth;
  if(!enif_inspect_binary(env, argv[0], &bin)) return make_error(env, "need_binary");
  if(!enif_get_uint(env, argv[1], &depth)) return make_error(env, "need_depth");

  unsigned shift = bin.size;

  struct BitReader br;
  bit_init(&br, bin.data, bin.size);
  
  ErlNifUInt64 timestamp;
  
  const ERL_NIF_TERM *prev;

  int32_t bid_prices[depth];
  uint32_t bid_sizes[depth];
  ERL_NIF_TERM bid[depth];

  int32_t ask_prices[depth];
  uint32_t ask_sizes[depth];
  ERL_NIF_TERM ask[depth];
  int i;
  
  int unpacking_delta = 0;
  
  ERL_NIF_TERM tag = enif_make_atom(env, "error");
  
  if(!bin.size) return make_error(env, "empty");
  
  
  int scale = 0;
  if(argc == 4) {
    if(!enif_get_int(env, argv[3], &scale)) scale = 0;
  }
  
  if(bit_get(&br)) {  // Decode full coded string
    if(bit_get(&br)) { // this means trade encoded
      if(bin.size < 8 + 2*4) return make_error(env, "more");
      
      tag = enif_make_atom(env, "trade");
      timestamp = bits_get(&br, 62);
      
      int32_t p = (int32_t)bits_get(&br, 32);
      ERL_NIF_TERM price = scale ? enif_make_double(env, p*1.0 / scale) : enif_make_int(env, p);
      ERL_NIF_TERM volume = enif_make_uint(env, (uint32_t)bits_get(&br, 32));
      shift = 8 + 2*4;
      return enif_make_tuple3(env,
        enif_make_atom(env, "ok"),
        enif_make_tuple4(env, tag, enif_make_uint64(env, timestamp), price, volume),
        enif_make_sub_binary(env, argv[0], shift, bin.size - shift)
        );
    }
    if(bin.size < 8 + 2*2*depth*4) return make_error(env, "more");
    
    // Here is decoding of simply coded full string
    tag = enif_make_atom(env, "md");
    timestamp = bits_get(&br, 62);
    for(i = 0; i < depth; i++) {
      bid_prices[i] = (int32_t)bits_get(&br, 32);
      bid_sizes[i] = (uint32_t)bits_get(&br, 32);
    }
    for(i = 0; i < depth; i++) {
      ask_prices[i] = (int32_t)bits_get(&br, 32);
      ask_sizes[i] = (uint32_t)bits_get(&br, 32);
    }
  } else {
    unpacking_delta = 0;
    tag = enif_make_atom(env, "delta");

    if(argc >= 3) {
      // And this means that we will append delta values to previous row
      unpacking_delta = 1;
      tag = enif_make_atom(env, "md");
      int arity = 0;
      if(!enif_get_tuple(env, argv[2], &arity, &prev)) return make_error(env, "need_prev");
      if(arity != 4) return make_error(env, "need_prev_arity_4");
    }
    
    timestamp = leb128_decode_unsigned(&br);
    for(i = 0; i < depth; i++) {
      bid_prices[i] = (int32_t)decode_delta(&br);
      bid_sizes[i] = (uint32_t)decode_delta(&br);
    }
    for(i = 0; i < depth; i++) {
      ask_prices[i] = (int32_t)decode_delta(&br);
      ask_sizes[i] = (uint32_t)decode_delta(&br);
    }
    
    if(unpacking_delta) {
      ErlNifUInt64 prev_ts;
      if(!enif_get_uint64(env, prev[1], &prev_ts)) return make_error(env, "need_prev_ts");
      timestamp += prev_ts;
      
      ERL_NIF_TERM head, tail;
      
      // add previous values to bid
      tail = prev[2];
      for(i = 0; i < depth; i++) {
        if(!enif_get_list_cell(env, tail, &head, &tail)) return make_error(env, "need_more_bid_in_prev");
        int ar = 0;
        const ERL_NIF_TERM *price_vol;
        if(!enif_get_tuple(env, head, &ar, &price_vol)) return make_error(env, "need_price_vol_in_prev_bid");
        if(ar != 2) return make_error(env, "need_price_vol_arity_2_in_prev_bid");
        
        int price, volume;
        double price_d;
        if(!enif_get_int(env, price_vol[0], &price)) {
          if(!scale) return make_error(env, "need_price_int_in_prev_bid");
          if(!enif_get_double(env, price_vol[0], &price_d)) return make_error(env, "need_price_double_in_prev_bid");
          price = price_d*scale;
        } 
        if(!enif_get_int(env, price_vol[1], &volume)) return make_error(env, "need_vol_int_in_prev_bid");
        bid_prices[i] += price;
        bid_sizes[i] += volume;
      }
      // and add previous values to ask
      tail = prev[3];
      for(i = 0; i < depth; i++) {
        if(!enif_get_list_cell(env, tail, &head, &tail)) return make_error(env, "need_more_ask_in_prev");
        int ar = 0;
        const ERL_NIF_TERM *price_vol;
        if(!enif_get_tuple(env, head, &ar, &price_vol)) return make_error(env, "need_price_vol_in_prev_ask");
        if(ar != 2) return make_error(env, "need_price_vol_arity_2_in_prev_ask");
        
        int price, volume;
        double price_d;
        if(!enif_get_int(env, price_vol[0], &price)) {
          if(!scale) return make_error(env, "need_price_int_in_prev_ask");
          if(!enif_get_double(env, price_vol[0], &price_d)) return make_error(env, "need_price_double_in_prev_ask");
          price = price_d*scale;
        } 
        if(!enif_get_int(env, price_vol[1], &volume)) return make_error(env, "need_vol_int_in_prev_ask");
        ask_prices[i] += price;
        ask_sizes[i] += volume;
      }
    }
    
  }
  
  bits_align(&br);
  shift = bits_byte_offset(&br);
  


  for(i = 0; i < depth; i++) {
    bid[i] = enif_make_tuple2(env,
      scale ? enif_make_double(env, bid_prices[i]*1.0 / scale) : enif_make_int(env, bid_prices[i]),
      enif_make_int(env, bid_sizes[i])
      );
    ask[i] = enif_make_tuple2(env,
      scale ? enif_make_double(env, ask_prices[i]*1.0 / scale) : enif_make_int(env, ask_prices[i]),
      enif_make_int(env, ask_sizes[i])
      );
  }
  
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
  {"read_one_row", 2, read_one_row},
  {"read_one_row", 3, read_one_row},
  {"read_one_row", 4, read_one_row}
};


ERL_NIF_INIT(stockdb_format, stockdb_funcs, NULL, reload, upgrade, NULL)
