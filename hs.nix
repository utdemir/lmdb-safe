{ mkDerivation, base, bytestring, cereal, directory, foldl, hspec
, HUnit, lmdb, mtl, pipes, pipes-concurrency, pointed, QuickCheck
, safecopy, stdenv, unix
}:
mkDerivation {
  pname = "lmdb-safe";
  version = "0.0.1.0";
  src = ./.;
  libraryHaskellDepends = [
    base bytestring cereal foldl lmdb mtl pipes pipes-concurrency
    pointed safecopy
  ];
  testHaskellDepends = [
    base bytestring directory hspec HUnit QuickCheck unix
  ];
  license = stdenv.lib.licenses.bsd3;
}
